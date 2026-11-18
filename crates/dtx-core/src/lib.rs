mod definition;
mod driver;
mod error;
mod listener;
mod manager;
mod status;
mod synchronization;
mod template;

pub use definition::{
    DefaultTransactionDefinition, Isolation, Propagation, StaticTransactionDefinition,
    TIMEOUT_DEFAULT, TransactionDefinition,
};
pub use driver::TransactionDriver;
pub use error::{BoxError, TransactionError};
pub use listener::TransactionExecutionListener;
pub use manager::{
    AbstractPlatformTransactionManager, PlatformTransactionManager, TransactionSynchronizationMode,
};
pub use status::{
    DefaultTransactionStatus, Savepoint, SavepointManager, TransactionExecution, TransactionStatus,
};
pub use synchronization::{
    BoundResource, CompletionStatus, ResourceHolder, ResourceKey, TransactionSynchronization,
    TransactionSynchronizationManager, TransactionSynchronizationUtils,
};
pub use template::{TemplateError, TransactionTemplate};

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
