use std::fmt;
use std::sync::Arc;

use crate::definition::TransactionDefinition;
use crate::error::TransactionError;
use crate::manager::PlatformTransactionManager;
use crate::status::TransactionStatus;
use crate::synchronization;
use crate::unwind::CatchUnwind;

#[derive(Debug)]
pub enum TemplateError<E> {
    Application(E),
    Transaction(TransactionError),
    Rollback { application: E, rollback: TransactionError },
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

pub struct TransactionTemplate {
    transaction_manager: Arc<dyn PlatformTransactionManager>,
    definition: TransactionDefinition,
}

impl TransactionTemplate {
    pub fn new(transaction_manager: Arc<dyn PlatformTransactionManager>) -> Self {
        Self { transaction_manager, definition: TransactionDefinition::new() }
    }

    pub fn with_definition(
        transaction_manager: Arc<dyn PlatformTransactionManager>,
        definition: TransactionDefinition,
    ) -> Self {
        Self { transaction_manager, definition }
    }

    pub fn definition(&self) -> &TransactionDefinition {
        &self.definition
    }

    pub fn definition_mut(&mut self) -> &mut TransactionDefinition {
        &mut self.definition
    }

    pub fn transaction_manager(&self) -> &Arc<dyn PlatformTransactionManager> {
        &self.transaction_manager
    }

    pub async fn without_transaction<T, E, F, Fut>(action: F) -> Result<T, E>
    where
        F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let status: Arc<dyn TransactionStatus> =
            Arc::new(crate::status::SimpleTransactionStatus::with_new_transaction(false));
        action(status).await
    }

    pub async fn execute<T, E, F, Fut>(&self, action: F) -> Result<T, TemplateError<E>>
    where
        F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        Box::pin(synchronization::ensure_scope(async {
            let status = self
                .transaction_manager
                .transaction(Some(&self.definition))
                .await
                .map_err(TemplateError::Transaction)?;
            let outcome = match CatchUnwind::new(synchronization::boundary_scope(
                status.boundary(),
                action(status.clone()),
            ))
            .await
            {
                Ok(outcome) => outcome,
                Err(panic) => {
                    if let Err(error) = self.transaction_manager.rollback(status.as_ref()).await {
                        tracing::error!(%error, "Application panic overridden by rollback error");
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
                        tracing::error!(error = %rollback, "Application exception overridden by rollback exception");
                        Err(TemplateError::Rollback { application, rollback })
                    }
                },
            }
        }))
        .await
    }
}
