//! The transaction driver for libraries that run a transaction on one database connection.
//!
//! A connection library is supported by implementing [`ConnectionBackend`]: how to obtain a
//! connection, how to begin, commit and roll back on it, and how to run a savepoint statement.
//! Everything else is here and is the same for every library: [`ConnectionSource`] hands out
//! the connection of the current transaction, [`ConnectionHolder`] keeps it bound to the
//! transaction context, and [`ConnectionDriver`] is the
//! [`TransactionDriver`](demarcate_core::TransactionDriver) that the transaction manager calls.
//!
//! Applications use a crate for their connection library, such as `demarcate-sqlx`. Depend on
//! this crate to support another connection library.

#![warn(missing_docs)]

mod backend;
mod driver;
mod holder;
mod settings;
mod source;

pub use backend::ConnectionBackend;
pub use driver::{ConnectionDriver, ConnectionTransactionObject, manager, manager_with_settings};
pub use holder::ConnectionHolder;
pub use settings::{PoolSettings, SettingsError, default_manager_settings};
pub use source::{CONNECTION_SYNCHRONIZATION_ORDER, ConnectionHandle, ConnectionSource};
