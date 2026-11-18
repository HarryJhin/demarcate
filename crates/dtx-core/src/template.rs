use std::fmt;
use std::sync::Arc;

use crate::definition::{DefaultTransactionDefinition, TransactionDefinition};
use crate::error::TransactionError;
use crate::manager::PlatformTransactionManager;
use crate::status::TransactionStatus;

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
    definition: DefaultTransactionDefinition,
}

impl TransactionTemplate {
    pub fn new(transaction_manager: Arc<dyn PlatformTransactionManager>) -> Self {
        Self { transaction_manager, definition: DefaultTransactionDefinition::new() }
    }

    pub fn with_definition(
        transaction_manager: Arc<dyn PlatformTransactionManager>,
        definition: &dyn TransactionDefinition,
    ) -> Self {
        Self { transaction_manager, definition: DefaultTransactionDefinition::from_definition(definition) }
    }

    pub fn definition(&self) -> &DefaultTransactionDefinition {
        &self.definition
    }

    pub fn definition_mut(&mut self) -> &mut DefaultTransactionDefinition {
        &mut self.definition
    }

    pub fn transaction_manager(&self) -> &Arc<dyn PlatformTransactionManager> {
        &self.transaction_manager
    }

    pub async fn execute<T, E, F, Fut>(&self, action: F) -> Result<T, TemplateError<E>>
    where
        F: FnOnce(Arc<dyn TransactionStatus>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let status =
            self.transaction_manager.get_transaction(Some(&self.definition)).await.map_err(TemplateError::Transaction)?;
        match action(status.clone()).await {
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
    }
}
