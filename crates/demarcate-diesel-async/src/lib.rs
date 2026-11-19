//! Transaction management for diesel-async on a deadpool pool.
//!
//! `TransactionalPool::connection` returns the connection of the current transaction, so that
//! data access code needs no transaction argument:
//! `query.execute(&mut *pool.connection().await?).await?`.
//!
//! The modules are [`postgres`] for `AsyncPgConnection` and [`mysql`] for
//! `AsyncMysqlConnection` (MySQL and MariaDB). Applications normally depend on the `demarcate`
//! crate with the feature `diesel-async-postgres` or `diesel-async-mysql`.

#![warn(missing_docs)]

use std::fmt;

use demarcate_core::Isolation;
use diesel_async::pooled_connection::deadpool::PoolError;

#[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    Postgres,
    #[cfg_attr(not(feature = "mysql"), allow(dead_code))]
    Mysql,
}

/// The error of a transaction step: from the pool or from a statement.
#[derive(Debug)]
pub enum Error {
    /// No connection could be obtained from the pool.
    Pool(PoolError),
    /// A statement failed.
    Query(diesel::result::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(error) => write!(f, "{error}"),
            Self::Query(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pool(error) => Some(error),
            Self::Query(error) => Some(error),
        }
    }
}

impl From<PoolError> for Error {
    fn from(error: PoolError) -> Self {
        Self::Pool(error)
    }
}

impl From<diesel::result::Error> for Error {
    fn from(error: diesel::result::Error) -> Self {
        Self::Query(error)
    }
}

#[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(dead_code))]
fn isolation_clause(isolation: Isolation) -> Option<&'static str> {
    match isolation {
        Isolation::Default => None,
        Isolation::ReadUncommitted => Some("ISOLATION LEVEL READ UNCOMMITTED"),
        Isolation::ReadCommitted => Some("ISOLATION LEVEL READ COMMITTED"),
        Isolation::RepeatableRead => Some("ISOLATION LEVEL REPEATABLE READ"),
        Isolation::Serializable => Some("ISOLATION LEVEL SERIALIZABLE"),
    }
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
macro_rules! diesel_driver {
    (connection: $connection:ty, kind: $kind:expr $(,)?) => {
        use std::fmt;
        use std::ops::{Deref, DerefMut};
        use std::time::Duration;

        use demarcate_connection::{ConnectionBackend, ConnectionDriver, ConnectionHandle, ConnectionSource};
        use demarcate_core::{ManagerSettings, ResourceKey, TransactionDefinition, TransactionError};
        use diesel_async::pooled_connection::deadpool::{Object, Pool};
        use diesel_async::{AnsiTransactionManager, SimpleAsyncConnection, TransactionManager};

        use crate::{Error, Kind, isolation_clause};

        pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

        const KIND: Kind = $kind;

        #[doc(hidden)]
        pub struct Conn {
            connection: Option<Object<$connection>>,
            in_transaction: bool,
        }

        impl Conn {
            fn connection(&mut self) -> &mut $connection {
                self.connection.as_mut().expect("connection present")
            }
            fn connection_ref(&self) -> &$connection {
                self.connection.as_ref().expect("connection present")
            }
        }

        impl Drop for Conn {
            fn drop(&mut self) {
                if self.in_transaction
                    && let Some(connection) = self.connection.take()
                {
                    drop(Object::take(connection));
                }
            }
        }

        /// The [`ConnectionBackend`] for diesel-async on a deadpool pool.
        ///
        /// The transaction begins through diesel's transaction manager, so that diesel knows that the
        /// connection is in a transaction: a `connection.transaction(..)` of diesel inside a
        /// transactional function runs on a savepoint.
        ///
        /// A connection that is dropped while it runs a transaction, for example because its call was
        /// cancelled, is closed and not returned to the pool.
        pub struct DieselBackend {
            pool: Pool<$connection>,
        }

        impl ConnectionBackend for DieselBackend {
            type Connection = Conn;
            type Error = Error;

            const NAME: &'static str = "demarcate-diesel-async";

            const UNUSABLE_MESSAGE: &'static str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
                 when a statement fails, and the failure was not returned from the transactional function. Return the \
                 error, or run the statement that may fail in a nested transaction (propagation = Nested)";

            async fn acquire(&self) -> Result<Conn, Error> {
                Ok(Conn { connection: Some(self.pool.get().await?), in_transaction: false })
            }

            async fn begin(&self, definition: &TransactionDefinition) -> Result<Conn, Error> {
                let mut started = Conn { connection: Some(self.pool.get().await?), in_transaction: true };
                let connection = started.connection();
                let isolation = isolation_clause(definition.isolation);
                let statement = match KIND {
                    Kind::Postgres => {
                        let mut statement = String::from("BEGIN");
                        if let Some(isolation) = isolation {
                            statement.push(' ');
                            statement.push_str(isolation);
                        }
                        if definition.read_only {
                            statement.push_str(" READ ONLY");
                        }
                        statement
                    }
                    Kind::Mysql => {
                        if let Some(isolation) = isolation {
                            connection.batch_execute(&format!("SET TRANSACTION {isolation}")).await?;
                        }
                        String::from(if definition.read_only { "START TRANSACTION READ ONLY" } else { "START TRANSACTION" })
                    }
                };
                AnsiTransactionManager::begin_transaction_sql::<$connection>(connection, &statement).await?;
                Ok(started)
            }

            fn in_transaction(connection: &Conn) -> bool {
                connection.in_transaction
            }

            async fn commit(&self, mut connection: Conn) -> Result<(), Error> {
                <AnsiTransactionManager as TransactionManager<$connection>>::commit_transaction(connection.connection()).await?;
                connection.in_transaction = false;
                Ok(())
            }

            async fn rollback(&self, mut connection: Conn) -> Result<(), Error> {
                <AnsiTransactionManager as TransactionManager<$connection>>::rollback_transaction(connection.connection()).await?;
                connection.in_transaction = false;
                Ok(())
            }

            async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), Error> {
                Ok(connection.connection().batch_execute(statement).await?)
            }

            async fn apply_timeout(&self, connection: &mut Conn, remaining: Duration) -> Result<(), Error> {
                if KIND != Kind::Postgres {
                    return Ok(());
                }
                let millis = remaining.as_millis().max(1);
                Ok(connection
                    .connection()
                    .batch_execute(&format!("select set_config('statement_timeout', '{millis}', true)"))
                    .await?)
            }

            fn taints_transaction(&self, error: &Error) -> bool {
                KIND == Kind::Postgres && matches!(error, Error::Query(diesel::result::Error::DatabaseError(..)))
            }

            fn ends_transaction(&self, error: &Error) -> bool {
                KIND == Kind::Mysql
                    && matches!(error, Error::Query(diesel::result::Error::DatabaseError(_, information))
                        if information.message().contains("Deadlock found")
                            || information.message().contains("Record has changed since last read"))
            }

            async fn is_usable(&self, connection: &mut Conn) -> bool {
                KIND != Kind::Postgres || connection.connection().batch_execute("select 1").await.is_ok()
            }

            fn release_failure_is_ignorable(&self, error: &Error) -> bool {
                matches!(error, Error::Query(diesel::result::Error::DatabaseError(_, information))
                    if information.message().to_lowercase().contains("savepoint")
                        && information.message().contains("does not exist"))
            }
        }

        /// The connection of one transaction, or of one synchronization scope without a transaction,
        /// bound to the transaction context under the key of its pool.
        pub type ConnectionHolder = demarcate_connection::ConnectionHolder<DieselBackend>;

        /// A diesel-async deadpool pool that hands out the connection of the current transaction.
        ///
        /// Inside a transaction [`connection`](TransactionalPool::connection) returns the connection of
        /// that transaction. Outside of one it returns a pooled connection in autocommit mode, unless
        /// the pool was made with [`transaction_required`](TransactionalPool::transaction_required).
        ///
        /// Drop the returned connection before the next call of `connection` and before calling another
        /// transactional function: while it is alive the connection is in use, and a second request for
        /// it returns an error.
        ///
        /// A transaction with a timeout checks its deadline each time a connection is handed out: a
        /// request after the deadline fails with [`TransactionError::TimedOut`]. On PostgreSQL it also
        /// sets `statement_timeout`, so that the server cancels a statement that is running at the
        /// deadline.
        ///
        /// The statements run on the connection itself, so their failures are not seen here. On
        /// PostgreSQL the commit therefore asks the server whether the transaction is usable and returns
        /// [`TransactionError::UnexpectedRollback`] if a failed statement has aborted it.
        ///
        /// Note that on MySQL and MariaDB a deadlock (SQLSTATE `40001`), and on MariaDB error 1020
        /// "Record has changed since last read", ends the transaction on the server and leaves the
        /// connection in autocommit mode. This is not detected, and the statements that follow run
        /// outside the transaction: return the error of the statement from the transactional function.
        ///
        /// Cloning is cheap; clones share the pool and the transaction.
        pub struct TransactionalPool {
            source: ConnectionSource<DieselBackend>,
        }

        impl Clone for TransactionalPool {
            fn clone(&self) -> Self {
                Self { source: self.source.clone() }
            }
        }

        impl From<Pool<$connection>> for TransactionalPool {
            fn from(pool: Pool<$connection>) -> Self {
                Self::new(pool)
            }
        }

        impl fmt::Debug for TransactionalPool {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("TransactionalPool")
                    .field("status", &self.inner().status())
                    .field("key", self.resource_key())
                    .field("transaction_required", &self.is_transaction_required())
                    .finish()
            }
        }

        impl TransactionalPool {
            /// Wraps a diesel-async deadpool pool.
            ///
            /// Create one `TransactionalPool` for each `Pool` and clone it. Two pools created from the
            /// same `Pool` do not see each other's transactions.
            pub fn new(pool: Pool<$connection>) -> Self {
                Self { source: ConnectionSource::new(DieselBackend { pool }) }
            }

            /// Returns a pool that refuses to hand out a connection outside a transaction.
            ///
            /// Give this pool to a repository whose functions must always run inside a transaction. A call
            /// without one returns [`TransactionError::NoTransaction`] instead of running in autocommit mode.
            pub fn transaction_required(&self) -> Self {
                Self { source: self.source.transaction_required() }
            }

            /// Returns whether this pool refuses to hand out a connection outside a transaction.
            pub fn is_transaction_required(&self) -> bool {
                self.source.is_transaction_required()
            }

            /// Returns the deadpool pool. A connection taken from it does not take part in any
            /// transaction.
            pub fn inner(&self) -> &Pool<$connection> {
                &self.source.backend().pool
            }

            /// Returns the key under which the connection of a transaction is bound to the context.
            pub fn resource_key(&self) -> &ResourceKey {
                self.source.resource_key()
            }

            /// Returns whether a connection of this pool is bound to the context of the calling future.
            pub fn is_connection_transactional(&self) -> bool {
                self.source.is_connection_transactional()
            }

            /// Returns the connection of the current transaction, or a pooled connection when there is
            /// none.
            ///
            /// Pass it to a diesel query as `&mut *connection`.
            ///
            /// # Errors
            ///
            /// Returns an error when the connection of the transaction is already in use, when a nested
            /// transaction of another call owns it, when the transaction that the calling future joined has
            /// completed, or when no connection can be obtained. Returns [`TransactionError::TimedOut`] when
            /// the deadline of the transaction has passed, and [`TransactionError::NoTransaction`] when the
            /// pool requires a transaction and none is running.
            pub async fn connection(&self) -> Result<TransactionalConnection, TransactionError> {
                let handle = self.source.acquire().await?;
                if KIND == Kind::Postgres
                    && let Some(holder) = handle.transaction_holder()
                {
                    holder.mark_aborted();
                }
                Ok(TransactionalConnection(handle))
            }
        }

        /// A connection obtained from a [`TransactionalPool`]: the connection of the current
        /// transaction, or a pooled connection.
        ///
        /// It dereferences to the diesel-async connection. Dropping it does not commit or roll back; the
        /// transaction manager completes the transaction.
        pub struct TransactionalConnection(ConnectionHandle<DieselBackend>);

        impl Deref for TransactionalConnection {
            type Target = $connection;
            fn deref(&self) -> &Self::Target {
                self.0.connection_ref()
            }
        }

        impl DerefMut for TransactionalConnection {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.0.connection()
            }
        }

        #[doc(hidden)]
        pub type DieselTransactionObject = demarcate_connection::ConnectionTransactionObject<DieselBackend>;

        /// The [`TransactionDriver`](demarcate_core::TransactionDriver) for diesel-async.
        ///
        /// A nested transaction is a savepoint on the connection of the outer transaction. While it
        /// runs, that connection belongs to the nested call, and a future beside it that uses the
        /// connection gets an error.
        pub type DieselTransactionDriver = ConnectionDriver<DieselBackend>;

        /// The transaction manager for diesel-async.
        pub type DieselTransactionManager = demarcate_core::TransactionManager<DieselTransactionDriver>;

        /// Creates a transaction manager for the given pool, with nested transactions allowed.
        ///
        /// The `install` function of this module in the `demarcate` crate creates and registers it in
        /// one call. Call this function to register the manager yourself:
        /// `demarcate::registry().set_default(Arc::new(manager(pool.clone())))`.
        pub fn manager(pool: TransactionalPool) -> DieselTransactionManager {
            demarcate_connection::manager(pool.source)
        }

        /// Creates a transaction manager for the given pool with the given settings.
        pub fn manager_with_settings(
            pool: TransactionalPool,
            settings: ManagerSettings,
        ) -> DieselTransactionManager {
            demarcate_connection::manager_with_settings(pool.source, settings)
        }
    };
}

/// The driver for PostgreSQL: `AsyncPgConnection`.
#[cfg(feature = "postgres")]
pub mod postgres {
    diesel_driver! { connection: diesel_async::AsyncPgConnection, kind: Kind::Postgres }
}

/// The driver for MySQL and MariaDB: `AsyncMysqlConnection`.
#[cfg(feature = "mysql")]
pub mod mysql {
    diesel_driver! { connection: diesel_async::AsyncMysqlConnection, kind: Kind::Mysql }
}
