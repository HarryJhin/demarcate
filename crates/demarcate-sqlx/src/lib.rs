//! Transaction management for sqlx.
//!
//! The [`postgres`] module contains a transaction manager for PostgreSQL and
//! [`TransactionalPool`](postgres::TransactionalPool), a pool that runs each query on the
//! connection of the current transaction, so that data access code needs no transaction
//! argument.
//!
//! Applications normally depend on the `demarcate` crate with the `sqlx-postgres` feature.

#![warn(missing_docs)]

/// The PostgreSQL driver and the transaction-aware pool.
#[cfg(feature = "postgres")]
pub mod postgres;
