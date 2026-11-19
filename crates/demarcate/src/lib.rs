//! Declarative transactions.
//!
//! Put [`#[transactional]`](macro@transactional) on a function, and the function runs in a
//! transaction: it commits when the function returns `Ok` and rolls back when it returns `Err`
//! or panics. Functions that it calls join the same transaction without a transaction
//! argument.
//!
//! ```ignore
//! use demarcate::transactional;
//!
//! struct Orders {
//!     pool: demarcate::postgres::TransactionalPool,
//! }
//!
//! impl Orders {
//!     #[transactional]
//!     pub async fn place(&self, item: &str) -> Result<(), Error> {
//!         self.insert(item).await?;
//!         self.audit("placed").await
//!     }
//!
//!     #[transactional(propagation = RequiresNew)]
//!     async fn audit(&self, event: &str) -> Result<(), Error> {
//!         sqlx::query("insert into audit values($1)").bind(event.to_owned()).execute(&self.pool).await?;
//!         Ok(())
//!     }
//!
//!     async fn insert(&self, item: &str) -> Result<(), Error> {
//!         sqlx::query("insert into orders values($1)").bind(item.to_owned()).execute(&self.pool).await?;
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Setup
//!
//! Enable the feature of your connection library, such as `sqlx-postgres`, and install the
//! transaction manager once at startup. The module of the driver has the functions:
//!
//! ```ignore
//! // Reads DATABASE_URL, connects the pool, creates the manager and registers it.
//! let pool = demarcate::postgres::install_from_env().await?;
//!
//! // Or with a pool that you built yourself.
//! let pool = demarcate::postgres::install(pg_pool)?;
//! ```
//!
//! Give the returned pool to the data access code. `install_from` takes a
//! [`PoolSettings`](connection::PoolSettings) value instead of the environment, and `install_as`
//! and `install_from_as` install the manager of a second database under a name for
//! `#[transactional(transaction_manager = "name")]`. To do the wiring yourself, create the
//! manager with `manager(pool)` and register it with [`registry`].
//!
//! The error type of a transactional function implements `From<TransactionError>`, because
//! beginning, committing and rolling back can fail.
//!
//! # What a transaction is bound to
//!
//! A transaction is bound to the call that started it. The futures of that call see it while
//! they are polled, wherever they are polled:
//!
//! - Futures that run beside each other in one function (`join!`, `select!`) are in the
//!   transaction of that function. The queries of one transaction still run one after another.
//! - A spawned task is not in the transaction of the task that spawned it. Start a new
//!   transaction inside the task.
//! - A call that is cancelled, because its future is dropped, is rolled back.
//!
//! # Where to look
//!
//! - [`#[transactional]`](macro@transactional) for the arguments of the attribute.
//! - [`Propagation`] for how a call relates to a transaction that is already running.
//! - [`RollbackRule`] for which errors roll back.
//! - [`TransactionTemplate`] to set a transaction boundary in code.
//! - [`on_transaction`] and [`TransactionSynchronization`] to run code at a phase of the
//!   transaction.
//! - [`connection::ConnectionBackend`] to support another connection library, and
//!   [`TransactionDriver`] for a resource that is not a database connection.

#![warn(missing_docs)]

#[macro_use]
mod install;

/// The transaction driver for libraries that run a transaction on one database connection.
/// Implement its `ConnectionBackend` to support another connection library.
pub use demarcate_connection as connection;
pub use demarcate_core::*;
pub use demarcate_interceptor::{
    FunctionRollbackEvent, RollbackRule, TransactionAttribute, TransactionInfo, TransactionManagerRegistry,
    current_transaction_info, current_transaction_status, error_chain_contains, invoke_with_manager,
    invoke_within_transaction, registry,
};
#[doc(hidden)]
pub use demarcate_interceptor::{TransactionalReturn, assert_transactional_return};
pub use demarcate_macros::transactional;
pub use install::{InstallError, SetupError};

/// The diesel-async drivers and the transaction-aware pools, on deadpool, for PostgreSQL, MySQL
/// and MariaDB.
#[cfg(any(feature = "diesel-async-postgres", feature = "diesel-async-mysql"))]
pub mod diesel_async {
    pub use demarcate_diesel_async::*;

    /// The driver for PostgreSQL: `AsyncPgConnection`.
    #[cfg(feature = "diesel-async-postgres")]
    pub mod postgres {
        pub use demarcate_diesel_async::postgres::*;

        install_functions! { pool: TransactionalPool, what: "a diesel-async deadpool `Pool`" }
    }

    /// The driver for MySQL and MariaDB: `AsyncMysqlConnection`.
    #[cfg(feature = "diesel-async-mysql")]
    pub mod mysql {
        pub use demarcate_diesel_async::mysql::*;

        install_functions! { pool: TransactionalPool, what: "a diesel-async deadpool `Pool`" }
    }
}

/// The SeaORM driver and the transaction-aware connection, for PostgreSQL, MySQL, MariaDB and
/// SQLite.
#[cfg(feature = "sea-orm")]
pub mod sea_orm {
    pub use demarcate_sea_orm::*;

    install_functions! { pool: TransactionalDatabase, what: "a SeaORM `DatabaseConnection`", connect: ConnectError }
}

/// The MySQL and MariaDB driver and the transaction-aware pool, through sqlx.
#[cfg(feature = "sqlx-mysql")]
pub mod mysql {
    pub use demarcate_sqlx::mysql::*;

    install_functions! { pool: TransactionalPool, what: "a sqlx `MySqlPool`", connect: ConnectError }
}

/// The PostgreSQL driver and the transaction-aware pool, through sqlx.
#[cfg(feature = "sqlx-postgres")]
pub mod postgres {
    pub use demarcate_sqlx::postgres::*;

    install_functions! { pool: TransactionalPool, what: "a sqlx `PgPool`", connect: ConnectError }
}

/// The SQLite driver and the transaction-aware pool, through sqlx.
#[cfg(feature = "sqlx-sqlite")]
pub mod sqlite {
    pub use demarcate_sqlx::sqlite::*;

    install_functions! { pool: TransactionalPool, what: "a sqlx `SqlitePool`", connect: ConnectError }
}

/// The tokio-postgres driver and the transaction-aware pool, on deadpool-postgres.
#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres {
    pub use demarcate_tokio_postgres::*;

    install_functions! { pool: TransactionalPool, what: "a deadpool-postgres `Pool`" }
}
