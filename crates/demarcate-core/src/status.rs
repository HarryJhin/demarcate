use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::BoxFuture;
use crate::driver::TransactionDriver;
use crate::error::TransactionError;
use crate::manager::SuspendedResourcesHolder;
use crate::synchronization;

pub type Savepoint = Arc<dyn Any + Send + Sync>;

pub trait TransactionExecution: Send + Sync {
    fn transaction_name(&self) -> String {
        String::new()
    }
    fn has_transaction(&self) -> bool {
        true
    }
    fn is_new_transaction(&self) -> bool {
        true
    }
    fn is_nested(&self) -> bool {
        false
    }
    fn is_read_only(&self) -> bool {
        false
    }
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        Err(TransactionError::InvalidState("setRollbackOnly not supported".into()))
    }
    fn is_rollback_only(&self) -> bool {
        false
    }
    fn is_completed(&self) -> bool {
        false
    }
}

pub trait SavepointManager: Send + Sync {
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>>;
    fn rollback_to_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>>;
    fn release_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>>;
}

pub trait TransactionStatus: TransactionExecution + SavepointManager + Any {
    fn has_savepoint(&self) -> bool {
        false
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async { Ok(()) })
    }
    #[doc(hidden)]
    fn boundary(&self) -> Option<u64> {
        None
    }
    fn as_any(&self) -> &dyn Any;
}

pub struct SimpleTransactionStatus {
    new_transaction: bool,
    rollback_only: AtomicBool,
    completed: AtomicBool,
}

impl SimpleTransactionStatus {
    pub fn new() -> Self {
        Self::with_new_transaction(true)
    }

    pub fn with_new_transaction(new_transaction: bool) -> Self {
        Self { new_transaction, rollback_only: AtomicBool::new(false), completed: AtomicBool::new(false) }
    }

    pub fn set_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

impl Default for SimpleTransactionStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionExecution for SimpleTransactionStatus {
    fn is_new_transaction(&self) -> bool {
        self.new_transaction
    }
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        if self.is_completed() {
            return Err(TransactionError::InvalidState("Transaction completed".into()));
        }
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
        Box::pin(async { Err(not_supported()) })
    }
    fn rollback_to_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async { Err(not_supported()) })
    }
    fn release_savepoint<'a>(&'a self, _savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async { Err(not_supported()) })
    }
}

impl TransactionStatus for SimpleTransactionStatus {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn not_supported() -> TransactionError {
    TransactionError::NestedNotSupported("This transaction does not support savepoints".into())
}

pub struct ManagedTransactionStatus<D: TransactionDriver> {
    driver: Arc<D>,
    transaction_name: Option<String>,
    transaction: Option<D::Transaction>,
    new_transaction: bool,
    new_synchronization: bool,
    nested: bool,
    read_only: bool,
    suspended_resources: Mutex<Option<SuspendedResourcesHolder<D>>>,
    rollback_only: AtomicBool,
    completed: AtomicBool,
    savepoint: Mutex<Option<Savepoint>>,
    boundary: Option<u64>,
    rollback_only_when_abandoned: bool,
}

impl<D: TransactionDriver> ManagedTransactionStatus<D> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        driver: Arc<D>,
        transaction_name: Option<String>,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        nested: bool,
        read_only: bool,
        suspended_resources: Option<SuspendedResourcesHolder<D>>,
        rollback_only_when_abandoned: bool,
    ) -> Self {
        Self {
            driver,
            transaction_name,
            transaction,
            new_transaction,
            new_synchronization,
            nested,
            read_only,
            suspended_resources: Mutex::new(suspended_resources),
            rollback_only: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            savepoint: Mutex::new(None),
            boundary: synchronization::enter_boundary(),
            rollback_only_when_abandoned,
        }
    }

    pub fn transaction(&self) -> Option<&D::Transaction> {
        self.transaction.as_ref()
    }

    pub fn is_new_synchronization(&self) -> bool {
        self.new_synchronization
    }

    pub fn is_local_rollback_only(&self) -> bool {
        self.rollback_only.load(Ordering::SeqCst)
    }

    pub fn is_global_rollback_only(&self) -> bool {
        self.transaction.as_ref().is_some_and(|transaction| self.driver.is_rollback_only(transaction))
    }

    pub(crate) fn set_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
        synchronization::exit_boundary(self.boundary);
    }

    pub(crate) fn is_in_order(&self) -> bool {
        synchronization::boundary_in_order(self.boundary)
    }

    pub(crate) fn take_suspended_resources(&self) -> Option<SuspendedResourcesHolder<D>> {
        self.suspended_resources.lock().expect("suspended resources lock").take()
    }

    fn held_savepoint(&self) -> Option<Savepoint> {
        self.savepoint.lock().expect("savepoint lock").clone()
    }

    fn set_savepoint(&self, savepoint: Option<Savepoint>) {
        *self.savepoint.lock().expect("savepoint lock") = savepoint;
    }

    pub(crate) async fn create_and_hold_savepoint(&self) -> Result<(), TransactionError> {
        let savepoint = self.driver.create_savepoint(self.require_savepoint_transaction()?).await?;
        synchronization::trigger_savepoint(&savepoint).await?;
        self.set_savepoint(Some(savepoint));
        Ok(())
    }

    pub(crate) async fn rollback_to_held_savepoint(&self) -> Result<(), TransactionError> {
        let savepoint = self.held_savepoint().ok_or_else(|| {
            TransactionError::Usage(
                "Cannot roll back to savepoint - no savepoint associated with current transaction".into(),
            )
        })?;
        synchronization::trigger_savepoint_rollback(&savepoint).await?;
        let transaction = self.require_savepoint_transaction()?;
        self.driver.rollback_to_savepoint(transaction, &savepoint).await?;
        self.driver.release_savepoint(transaction, &savepoint).await?;
        self.set_savepoint(None);
        Ok(())
    }

    pub(crate) async fn release_held_savepoint(&self) -> Result<(), TransactionError> {
        let savepoint = self.held_savepoint().ok_or_else(|| {
            TransactionError::Usage(
                "Cannot release savepoint - no savepoint associated with current transaction".into(),
            )
        })?;
        self.driver.release_savepoint(self.require_savepoint_transaction()?, &savepoint).await?;
        self.set_savepoint(None);
        Ok(())
    }

    fn require_savepoint_transaction(&self) -> Result<&D::Transaction, TransactionError> {
        self.transaction.as_ref().ok_or_else(|| {
            TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())
        })
    }
}

impl<D: TransactionDriver> TransactionExecution for ManagedTransactionStatus<D> {
    fn transaction_name(&self) -> String {
        self.transaction_name.clone().unwrap_or_default()
    }
    fn has_transaction(&self) -> bool {
        self.transaction.is_some()
    }
    fn is_new_transaction(&self) -> bool {
        self.has_transaction() && self.new_transaction
    }
    fn is_nested(&self) -> bool {
        self.nested
    }
    fn is_read_only(&self) -> bool {
        self.read_only
    }
    fn set_rollback_only(&self) -> Result<(), TransactionError> {
        if self.is_completed() {
            return Err(TransactionError::InvalidState("Transaction completed".into()));
        }
        self.rollback_only.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn is_rollback_only(&self) -> bool {
        self.is_local_rollback_only() || self.is_global_rollback_only()
    }
    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl<D: TransactionDriver> SavepointManager for ManagedTransactionStatus<D> {
    fn create_savepoint(&self) -> BoxFuture<'_, Result<Savepoint, TransactionError>> {
        Box::pin(async move {
            let savepoint = self.driver.create_savepoint(self.require_savepoint_transaction()?).await?;
            synchronization::trigger_savepoint(&savepoint).await?;
            Ok(savepoint)
        })
    }
    fn rollback_to_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move {
            synchronization::trigger_savepoint_rollback(savepoint).await?;
            self.driver.rollback_to_savepoint(self.require_savepoint_transaction()?, savepoint).await
        })
    }
    fn release_savepoint<'a>(&'a self, savepoint: &'a Savepoint) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move { self.driver.release_savepoint(self.require_savepoint_transaction()?, savepoint).await })
    }
}

impl<D: TransactionDriver> TransactionStatus for ManagedTransactionStatus<D> {
    fn has_savepoint(&self) -> bool {
        self.held_savepoint().is_some()
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async move {
            match self.transaction.as_ref() {
                Some(transaction) => self.driver.flush(transaction).await,
                None => Ok(()),
            }
        })
    }
    fn boundary(&self) -> Option<u64> {
        self.boundary
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<D: TransactionDriver> Drop for ManagedTransactionStatus<D> {
    fn drop(&mut self) {
        if self.is_completed() {
            return;
        }
        synchronization::exit_boundary(self.boundary);
        if self.new_synchronization {
            synchronization::clear();
        }
        if let Some(transaction) = &self.transaction {
            if self.new_transaction {
                self.driver.abandon(transaction);
            } else if self.rollback_only_when_abandoned
                && let Err(error) = self.driver.set_rollback_only(transaction)
            {
                tracing::debug!(%error, "Could not mark the abandoned participating transaction as rollback-only");
            }
        }
        if let Some(suspended) = self.take_suspended_resources() {
            suspended.restore_abandoned(self.driver.as_ref());
        }
    }
}
