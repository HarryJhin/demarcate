use std::sync::Arc;

use crate::definition::TransactionDefinition;
use crate::error::TransactionError;
use crate::manager::ManagerSettings;
use crate::status::Savepoint;
use crate::synchronization::{self, CompletionStatus, TransactionSynchronization};

/// The operations that a [`TransactionManager`](crate::TransactionManager) needs from one kind of
/// transactional resource.
///
/// The manager implements the propagation rules, suspension, the completion sequence and the
/// synchronization callbacks. A driver implements the steps that depend on the resource: begin,
/// commit, rollback, suspend, resume and savepoints. Implement this trait to support another
/// database library.
///
/// A driver binds its resource, typically a connection holder, to the transaction context with
/// [`synchronization::bind_resource`], and data access code finds it there.
///
/// Only [`transaction`](TransactionDriver::transaction), [`begin`](TransactionDriver::begin),
/// [`commit`](TransactionDriver::commit) and [`rollback`](TransactionDriver::rollback) are
/// required. The default of every other method fits a driver that supports neither joining an
/// existing transaction, nor suspension, nor savepoints.
pub trait TransactionDriver: Send + Sync + 'static {
    /// The transaction object: the driver's view of the transaction state for one call.
    type Transaction: Send + Sync + 'static;
    /// The resources of a suspended transaction, kept until the transaction resumes.
    type Suspended: Send + 'static;

    /// Returns a transaction object for the current state of the context.
    ///
    /// The object refers to the transaction that is already bound to the context, if there is one.
    /// This method must not start a transaction.
    fn transaction(&self, settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError>;

    /// Returns whether the transaction object refers to a transaction that is already running.
    ///
    /// The manager uses the answer to apply the propagation mode. The default is `false`.
    fn is_existing_transaction(&self, _transaction: &Self::Transaction) -> bool {
        false
    }

    /// Returns whether a nested transaction is a savepoint in the existing transaction.
    ///
    /// When `false`, the manager calls [`begin`](TransactionDriver::begin) again for a nested call
    /// and the driver nests the transaction itself. The default is `true`.
    fn use_savepoint_for_nested_transaction(&self) -> bool {
        true
    }

    /// Starts a new transaction with the given definition and binds its resource to the context.
    ///
    /// The manager calls this only when no transaction is running, or after it suspended the running
    /// one. Apply the isolation level, the timeout and the read-only flag here.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::CannotCreateTransaction`] when the transaction cannot be started.
    fn begin(
        &self,
        transaction: &Self::Transaction,
        definition: &TransactionDefinition,
        settings: &ManagerSettings,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send;

    /// Suspends the transaction: unbinds its resource from the context and returns it.
    ///
    /// The default fails with [`TransactionError::SuspensionNotSupported`].
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

    /// Resumes a suspended transaction: binds the given resource to the context again.
    ///
    /// The default fails with [`TransactionError::SuspensionNotSupported`].
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

    /// Returns whether the manager calls [`commit`](TransactionDriver::commit) on a transaction that
    /// is marked rollback-only, leaving the decision to the resource.
    ///
    /// The default is `false`: the manager rolls such a transaction back.
    fn should_commit_on_global_rollback_only(&self) -> bool {
        false
    }

    /// Called before the commit sequence starts, before the `before_commit` callbacks.
    fn prepare_for_commit(
        &self,
        _transaction: Option<&Self::Transaction>,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Ok(()) }
    }

    /// Commits the transaction.
    ///
    /// The manager has already checked the rollback-only marks and run the callbacks that precede
    /// the commit.
    fn commit(&self, transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send;

    /// Rolls the transaction back.
    fn rollback(&self, transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send;

    /// Marks the existing transaction rollback-only, on behalf of a call that joined it and failed.
    ///
    /// A driver whose [`is_existing_transaction`](TransactionDriver::is_existing_transaction) can
    /// return `true` must implement this. The default returns an error.
    fn set_rollback_only(&self, _transaction: &Self::Transaction) -> Result<(), TransactionError> {
        Err(TransactionError::InvalidTransactionState(
            "Participating in existing transactions is not supported - when 'isExistingTransaction' \
             returns true, appropriate 'doSetRollbackOnly' behavior must be provided"
                .into(),
        ))
    }

    /// Arranges for the given synchronizations to be told the outcome of an existing transaction
    /// that this manager joined and does not control.
    ///
    /// The default invokes them immediately with [`CompletionStatus::Unknown`].
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

    /// Releases the resource after the transaction completed: unbinds it from the context and
    /// returns the connection.
    fn cleanup_after_completion(&self, _transaction: &Self::Transaction) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Releases the resource of a transaction whose call was cancelled before it completed.
    ///
    /// This runs in a destructor, so it cannot await and must not use the transaction context. Roll
    /// the transaction back by dropping or closing the connection.
    fn abandon(&self, _transaction: &Self::Transaction) {}

    /// Forgets the savepoint of a nested call that was cancelled before it completed.
    ///
    /// This runs in a destructor, like [`abandon`](TransactionDriver::abandon).
    fn abandon_savepoint(&self, _transaction: &Self::Transaction, _savepoint: &Savepoint) {}

    /// Records that the given savepoint belongs to the nested call that is starting now.
    ///
    /// A driver whose nested transactions share the connection of the outer transaction uses this to
    /// refuse work from futures that run beside the nested call, until the savepoint is released.
    fn own_savepoint(&self, _transaction: &Self::Transaction, _savepoint: &Savepoint) {}

    /// Marks the existing transaction rollback-only because a call that joined it was cancelled.
    ///
    /// The mark must survive a later rollback to a savepoint. The default calls
    /// [`set_rollback_only`](TransactionDriver::set_rollback_only).
    fn set_rollback_only_on_abandon(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        self.set_rollback_only(transaction)
    }

    /// Returns whether the transaction is marked rollback-only. The default is `false`.
    fn is_rollback_only(&self, _transaction: &Self::Transaction) -> bool {
        false
    }

    /// Flushes pending changes of the underlying session to the database.
    fn flush(&self, _transaction: &Self::Transaction) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Ok(()) }
    }

    /// Creates a savepoint in the transaction.
    ///
    /// The default fails with [`TransactionError::NestedNotSupported`].
    fn create_savepoint(
        &self,
        _transaction: &Self::Transaction,
    ) -> impl Future<Output = Result<Savepoint, TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }

    /// Rolls the transaction back to the given savepoint.
    ///
    /// The default fails with [`TransactionError::NestedNotSupported`].
    fn rollback_to_savepoint(
        &self,
        _transaction: &Self::Transaction,
        _savepoint: &Savepoint,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }

    /// Releases the given savepoint.
    ///
    /// The default fails with [`TransactionError::NestedNotSupported`].
    fn release_savepoint(
        &self,
        _transaction: &Self::Transaction,
        _savepoint: &Savepoint,
    ) -> impl Future<Output = Result<(), TransactionError>> + Send {
        async { Err(TransactionError::NestedNotSupported("Transaction object does not support savepoints".into())) }
    }
}
