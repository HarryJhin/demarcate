mod definition;
mod driver;
mod error;
mod event;
mod holder;
mod listener;
mod manager;
mod status;
pub mod synchronization;
mod template;
mod unwind;

pub use definition::{Isolation, Propagation, TransactionDefinition};
pub use driver::TransactionDriver;
pub use error::{BoxError, TransactionError};
pub use event::{TransactionPhase, on_transaction};
pub use holder::ResourceHolderSupport;
pub use listener::TransactionExecutionListener;
pub use manager::{ManagerSettings, PlatformTransactionManager, SynchronizationMode, TransactionManager};
#[doc(hidden)]
pub use status::AbandonOnDrop;
pub use status::{
    ManagedTransactionStatus, Savepoint, SavepointManager, SimpleTransactionStatus, TransactionExecution,
    TransactionStatus,
};
pub use synchronization::{BoundResource, CompletionStatus, ResourceHolder, ResourceKey, TransactionSynchronization};
pub use template::{TemplateError, TransactionTemplate};
pub use unwind::CatchUnwind;

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
