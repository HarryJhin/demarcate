//! Transaction management for sqlx.
//!
//! Each database module contains a transaction manager and a `TransactionalPool`, a pool that
//! runs each query on the connection of the current transaction, so that data access code needs
//! no transaction argument: [`postgres`], [`mysql`] (MySQL and MariaDB) and [`sqlite`].
//!
//! Applications normally depend on the `demarcate` crate with the feature `sqlx-postgres`,
//! `sqlx-mysql` or `sqlx-sqlite`.

#![warn(missing_docs)]

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
#[macro_use]
mod macros;

/// The MySQL and MariaDB driver and the transaction-aware pool.
#[cfg(feature = "mysql")]
pub mod mysql;
/// The PostgreSQL driver and the transaction-aware pool.
#[cfg(feature = "postgres")]
pub mod postgres;
/// The SQLite driver and the transaction-aware pool.
#[cfg(feature = "sqlite")]
pub mod sqlite;
