use crate::error::TransactionError;
use crate::status::TransactionExecution;

pub trait TransactionExecutionListener: Send + Sync {
    fn before_begin(&self, _transaction: &dyn TransactionExecution) {}
    fn after_begin(&self, _transaction: &dyn TransactionExecution, _begin_failure: Option<&TransactionError>) {}
    fn before_commit(&self, _transaction: &dyn TransactionExecution) {}
    fn after_commit(&self, _transaction: &dyn TransactionExecution, _commit_failure: Option<&TransactionError>) {}
    fn before_rollback(&self, _transaction: &dyn TransactionExecution) {}
    fn after_rollback(&self, _transaction: &dyn TransactionExecution, _rollback_failure: Option<&TransactionError>) {}
}
