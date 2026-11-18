use crate::error::TransactionError;
use crate::status::TransactionExecution;

/// A callback for the begin, commit and rollback steps of a transaction manager.
///
/// A listener is registered on a manager with
/// [`TransactionManager::add_listener`](crate::TransactionManager::add_listener) and sees every
/// transaction of that manager. It observes; it cannot change the outcome. To run code as part of
/// one particular transaction, register a
/// [`TransactionSynchronization`](crate::TransactionSynchronization) instead.
///
/// The begin and commit callbacks run only for a call that starts an actual transaction or a
/// nested one; a call that joins an existing transaction triggers none of them. When a commit
/// fails and the manager rolls the transaction back, `after_rollback` is called instead of
/// `after_commit`, without a preceding `before_rollback`, and a call that joined the transaction
/// can receive it too.
///
/// Every method has an empty default implementation.
pub trait TransactionExecutionListener: Send + Sync {
    /// Called before the transaction begins.
    fn before_begin(&self, _transaction: &dyn TransactionExecution) {}
    /// Called after the transaction began, or failed to begin.
    ///
    /// `begin_failure` is the error when the begin failed. Note that this is not called when the
    /// transaction began and the synchronization scope could not be opened.
    fn after_begin(&self, _transaction: &dyn TransactionExecution, _begin_failure: Option<&TransactionError>) {}
    /// Called before the transaction commits.
    fn before_commit(&self, _transaction: &dyn TransactionExecution) {}
    /// Called after the transaction committed, or failed to commit.
    ///
    /// `commit_failure` is the error when the commit failed.
    fn after_commit(&self, _transaction: &dyn TransactionExecution, _commit_failure: Option<&TransactionError>) {}
    /// Called before the transaction rolls back.
    fn before_rollback(&self, _transaction: &dyn TransactionExecution) {}
    /// Called after the transaction rolled back, or failed to roll back.
    ///
    /// `rollback_failure` is the error when the rollback failed.
    fn after_rollback(&self, _transaction: &dyn TransactionExecution, _rollback_failure: Option<&TransactionError>) {}
}
