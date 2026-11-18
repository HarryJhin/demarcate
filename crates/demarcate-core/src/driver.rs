use std::sync::Arc;

use crate::definition::TransactionDefinition;
use crate::error::TransactionError;
use crate::manager::ManagerSettings;
use crate::status::Savepoint;
use crate::synchronization::{self, CompletionStatus, TransactionSynchronization};

pub trait TransactionDriver: Send + Sync + 'static {
    type Transaction: Send + Sync + 'static;
    type Suspended: Send + 'static;

    fn transaction(&self, settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError>;

    fn is_existing_transaction(&self, _transaction: &Self::Transaction) -> bool {
        false
    }

    fn use_savepoint_for_nested_transaction(&self) -> bool {
        true
    }

    fn begin(
        &self,
        transaction: &Self::Transaction,
        definition: &TransactionDefinition,
        settings: &ManagerSettings,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send;

    fn suspend(
        &self,
        _transaction: &Self::Transaction,
    ) -> impl Future<Output = Result<Self::Suspended, TransactionError>> + Send {
        async {
            Err(TransactionError::SuspensionNotSupported(
                "Transaction manager does not support transaction suspension".into(),
            ))
        }
    }

    fn resume(
        &self,
        _transaction: Option<&Self::Transaction>,
        _suspended: Self::Suspended,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async {
            Err(TransactionError::SuspensionNotSupported(
                "Transaction manager does not support transaction suspension".into(),
            ))
        }
    }

    fn should_commit_on_global_rollback_only(&self) -> bool {
        false
    }

    fn prepare_for_commit(
        &self,
        _transaction: Option<&Self::Transaction>,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Ok(()) }
    }

    fn commit(&self, transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send;

    fn rollback(&self, transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send;

    fn set_rollback_only(&self, _transaction: &Self::Transaction) -> Result<(), TransactionError> {
        Err(TransactionError::InvalidTransactionState(
            "Participating in existing transactions is not supported - when 'isExistingTransaction' \
             returns true, appropriate 'doSetRollbackOnly' behavior must be provided"
                .into(),
        ))
    }

    fn register_after_completion_with_existing_transaction(
        &self,
        _transaction: &Self::Transaction,
        synchronizations: Vec<Arc<dyn TransactionSynchronization>>,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async move {
            synchronization::invoke_after_completion(&synchronizations, CompletionStatus::Unknown).await;
            Ok(())
        }
    }

    fn cleanup_after_completion(&self, _transaction: &Self::Transaction) -> impl Future<Output = ()> + Send {
        async {}
    }

    fn abandon(&self, _transaction: &Self::Transaction) {}

    fn abandon_savepoint(&self, _transaction: &Self::Transaction, _savepoint: &Savepoint) {}

    fn is_rollback_only(&self, _transaction: &Self::Transaction) -> bool {
        false
    }

    fn flush(&self, _transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Ok(()) }
    }

    fn create_savepoint(
        &self,
        _transaction: &Self::Transaction,
    ) -> impl Future<Output = Result<Savepoint, TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }

    fn rollback_to_savepoint(
        &self,
        _transaction: &Self::Transaction,
        _savepoint: &Savepoint,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }

    fn release_savepoint(
        &self,
        _transaction: &Self::Transaction,
        _savepoint: &Savepoint,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }
}
