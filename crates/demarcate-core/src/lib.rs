//! The transaction abstraction: definitions, propagation, transaction status, the transaction
//! context and the transaction manager.
//!
//! This crate contains what does not depend on a database library:
//!
//! - [`TransactionDefinition`], [`Propagation`] and [`Isolation`] describe a transaction.
//! - [`PlatformTransactionManager`] begins, commits and rolls back. [`TransactionManager`]
//!   implements it with the propagation rules, on top of a [`TransactionDriver`] that supplies
//!   the steps of one kind of resource.
//! - [`TransactionStatus`] is the handle to one transactional call.
//! - [`synchronization`] is the transaction context of the calling future: the resources bound
//!   to the transaction and the [`TransactionSynchronization`] callbacks registered with it.
//! - [`TransactionTemplate`] runs a closure in a transaction, and [`on_transaction`] registers a
//!   listener for a phase of the current transaction.
//!
//! The context is bound to a call, not to a thread or a task. Every transactional call sees its
//! own context while its future is polled, so futures that one task runs beside each other keep
//! the transaction of the function they were written in. No async runtime is required; the
//! `tokio` feature adds [`synchronization::spawn`].
//!
//! Applications normally depend on the `demarcate` crate, which adds the `#[transactional]`
//! attribute and the drivers. Depend on this crate directly to write a driver.

#![warn(missing_docs)]

mod definition;
mod driver;
mod error;
mod event;
mod holder;
mod listener;
mod manager;
mod status;
/// The transaction context of the calling future: bound resources, registered synchronizations
/// and the properties of the current transaction.
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

/// A boxed future that can be sent across threads.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
