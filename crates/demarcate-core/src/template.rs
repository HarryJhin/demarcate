use std::fmt;
use std::sync::Arc;

use crate::definition::TransactionDefinition;
use crate::error::TransactionError;
use crate::manager::PlatformTransactionManager;
use crate::status::TransactionStatus;
use crate::synchronization;
use crate::unwind::CatchUnwind;

/// The error of [`TransactionTemplate::execute`]: the error of the action, or an error of the
/// transaction infrastructure.
#[derive(Debug)]
pub enum TemplateError<E> {
    /// The action returned this error and the transaction was rolled back.
    Application(E),
    /// The transaction could not be started or completed. The action either did not run or returned
    /// `Ok`. Note that when the error comes from an `after_commit` callback, the transaction is
    /// committed.
    Transaction(TransactionError),
    /// The action returned an error and the rollback failed as well.
    Rollback {
        /// The error of the action.
        application: E,
        /// The error of the rollback.
        rollback: TransactionError,
    },
}

impl<E: fmt::Display> fmt::Display for TemplateError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Application(error) => error.fmt(f),
            Self::Transaction(error) => error.fmt(f),
            Self::Rollback { rollback, .. } => rollback.fmt(f),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for TemplateError<E> {}

/// Runs a closure in a transaction, for code that sets the boundary programmatically.
///
/// [`execute`](TransactionTemplate::execute) starts or joins a transaction as the definition
/// says, runs the action, and commits when the action returns `Ok`. It rolls back when the action
/// returns `Err` or panics. This is the programmatic counterpart of `#[transactional]`, and like
/// it gives the call its own transaction context.
///
/// A template holds a manager and a definition and can be reused for any number of calls.
pub struct TransactionTemplate {
    transaction_manager: Arc<dyn PlatformTransactionManager>,
    definition: TransactionDefinition,
}

impl TransactionTemplate {
    /// Creates a template with the default definition.
    pub fn new(transaction_manager: Arc<dyn PlatformTransactionManager>) -> Self {
        Self { transaction_manager, definition: TransactionDefinition::new() }
    }

    /// Creates a template with the given definition.
    pub fn with_definition(
        transaction_manager: Arc<dyn PlatformTransactionManager>,
        definition: TransactionDefinition,
    ) -> Self {
        Self { transaction_manager, definition }
    }

    /// Returns the definition.
    pub fn definition(&self) -> &TransactionDefinition {
        &self.definition
    }

    /// Returns the definition for modification.
    pub fn definition_mut(&mut self) -> &mut TransactionDefinition {
        &mut self.definition
    }

    /// Returns the transaction manager.
    pub fn transaction_manager(&self) -> &Arc<dyn PlatformTransactionManager> {
        &self.transaction_manager
    }

    /// Runs the action without a transaction manager.
    ///
    /// The action receives a status that reports no new transaction. Use this to run code written
    /// for [`execute`](TransactionTemplate::execute) where no transaction is wanted.
    pub async fn without_transaction<T, E, F, Fut>(action: F) -> Result<T, E>
    where
        F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let status: Arc<dyn TransactionStatus> =
            Arc::new(crate::status::SimpleTransactionStatus::with_new_transaction(false));
        action(status).await
    }

    /// Runs the action in a transaction and returns its value.
    ///
    /// The action receives the [`TransactionStatus`], on which it can call
    /// [`set_rollback_only`](crate::TransactionExecution::set_rollback_only) to roll back while
    /// returning `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::Application`] with the error of the action after a rollback,
    /// [`TemplateError::Transaction`] when the transaction could not be started or committed, and
    /// [`TemplateError::Rollback`] when the rollback after a failed action failed too.
    ///
    /// # Panics
    ///
    /// A panic of the action rolls the transaction back and then continues to unwind.
    pub async fn execute<T, E, F, Fut>(&self, action: F) -> Result<T, TemplateError<E>>
    where
        F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        Box::pin(synchronization::call_scope(async {
            let status = self
                .transaction_manager
                .transaction(Some(&self.definition))
                .await
                .map_err(TemplateError::Transaction)?;
            let _abandon = crate::AbandonOnDrop(status.clone());
            let outcome = match CatchUnwind::new(action(status.clone())).await {
                Ok(outcome) => outcome,
                Err(panic) => {
                    if let Err(error) = self.transaction_manager.rollback(status.as_ref()).await {
                        tracing::error!(%error, "The rollback after a panic failed");
                    }
                    std::panic::resume_unwind(panic)
                }
            };
            match outcome {
                Ok(value) => {
                    self.transaction_manager.commit(status.as_ref()).await.map_err(TemplateError::Transaction)?;
                    Ok(value)
                }
                Err(application) => match self.transaction_manager.rollback(status.as_ref()).await {
                    Ok(()) => Err(TemplateError::Application(application)),
                    Err(rollback) => {
                        tracing::error!(error = %rollback, "The rollback after an application error failed");
                        Err(TemplateError::Rollback { application, rollback })
                    }
                },
            }
        }))
        .await
    }
}
