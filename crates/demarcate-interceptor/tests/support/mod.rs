#![allow(dead_code)]

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use demarcate_core::{
    BoxFuture, PlatformTransactionManager, Savepoint, SavepointManager, TransactionDefinition, TransactionError,
    TransactionExecution, TransactionStatus,
};

#[derive(Default)]
pub struct SimpleTransactionStatus {
    rollback_only: AtomicBool,
    completed: AtomicBool,
}

impl TransactionExecution for SimpleTransactionStatus {
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        self.rollback_only.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn is_rollback_only(&self) -> bool {
        self.rollback_only.load(Ordering::SeqCst)
    }
    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl SavepointManager for SimpleTransactionStatus {
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>> {
        Box::pin(async {
            Err(TransactionError::NestedNotSupported("This transaction does not support savepoints".into()))
        })
    }
    fn rollback_to_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async {
            Err(TransactionError::NestedNotSupported("This transaction does not support savepoints".into()))
        })
    }
    fn release_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async {
            Err(TransactionError::NestedNotSupported("This transaction does not support savepoints".into()))
        })
    }
}

impl TransactionStatus for SimpleTransactionStatus {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub struct CallCountingTransactionManager {
    pub begun: AtomicUsize,
    pub commits: AtomicUsize,
    pub rollbacks: AtomicUsize,
    pub last_definition: Mutex<Option<TransactionDefinition>>,
    pub fail_begin: AtomicBool,
    pub fail_commit: AtomicBool,
    pub fail_commit_with_system_exception: AtomicBool,
    pub fail_rollback: AtomicBool,
}

impl CallCountingTransactionManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn counts(&self) -> [usize; 3] {
        [self.begun.load(Ordering::SeqCst), self.commits.load(Ordering::SeqCst), self.rollbacks.load(Ordering::SeqCst)]
    }
    pub fn definition(&self) -> TransactionDefinition {
        self.last_definition.lock().unwrap().clone().expect("a transaction was begun")
    }
}

impl PlatformTransactionManager for CallCountingTransactionManager {
    fn transaction<'a>(
        &'a self,
        definition: Option<&'a TransactionDefinition>,
    ) -> BoxFuture<'a, Result<Arc<dyn TransactionStatus>, TransactionError>> {
        Box::pin(async move {
            if self.fail_begin.load(Ordering::SeqCst) {
                return Err(TransactionError::CannotCreateTransaction {
                    message: "Cannot create".into(),
                    source: None,
                });
            }
            self.begun.fetch_add(1, Ordering::SeqCst);
            *self.last_definition.lock().unwrap() = definition.cloned();
            let status: Arc<dyn TransactionStatus> = Arc::new(SimpleTransactionStatus::default());
            Ok(status)
        })
    }

    fn commit<'a>(&'a self, _status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.fail_commit_with_system_exception.load(Ordering::SeqCst) {
                return Err(TransactionError::System {
                    message: "system exception".into(),
                    source: None,
                    application: None,
                });
            }
            if self.fail_commit.load(Ordering::SeqCst) {
                return Err(TransactionError::UnexpectedRollback("Cannot commit".into()));
            }
            Ok(())
        })
    }

    fn rollback<'a>(&'a self, _status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
            if self.fail_rollback.load(Ordering::SeqCst) {
                return Err(TransactionError::System {
                    message: "system exception".into(),
                    source: None,
                    application: None,
                });
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
pub enum AppError {
    Transaction(TransactionError),
    Business(&'static str),
}

impl From<TransactionError> for AppError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

#[derive(Debug)]
pub enum OtherError {
    Transaction(TransactionError),
    Failed,
}

impl From<TransactionError> for OtherError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}
