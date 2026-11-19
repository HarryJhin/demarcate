//! Transaction management for tokio-postgres on a deadpool-postgres pool.
//!
//! [`TransactionalPool::client`] returns the client of the current transaction, so that data
//! access code needs no transaction argument:
//! `pool.client().await?.execute("insert ...", &[&value]).await?`.
//!
//! Applications normally depend on the `demarcate` crate with the `tokio-postgres` feature.

#![warn(missing_docs)]

use std::ops::Deref;
use std::time::Duration;

use deadpool_postgres::{Object, Pool, PoolError};
use demarcate_connection::{ConnectionBackend, ConnectionDriver, ConnectionHandle, ConnectionSource};
use demarcate_core::{
    Isolation, ManagerSettings, ResourceKey, TransactionDefinition, TransactionError, TransactionManager,
};

pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

#[doc(hidden)]
pub struct Conn {
    client: Option<Object>,
    in_transaction: bool,
}

impl Conn {
    fn client(&self) -> &Object {
        self.client.as_ref().expect("client present")
    }

    async fn finish(mut self, statement: &str) -> Result<(), PoolError> {
        self.client().batch_execute(statement).await?;
        self.in_transaction = false;
        Ok(())
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if self.in_transaction
            && let Some(client) = self.client.take()
        {
            drop(Object::take(client));
        }
    }
}

/// The [`ConnectionBackend`] for tokio-postgres on a deadpool-postgres pool.
///
/// A read-only transaction begins as `READ ONLY`, so PostgreSQL rejects a write in it.
///
/// A client that is dropped while it runs a transaction, for example because its call was
/// cancelled, is closed and not returned to the pool. The server rolls the transaction back,
/// and the pool opens a new connection when it needs one.
pub struct TokioPostgresBackend {
    pool: Pool,
}

fn begin_statement(definition: &TransactionDefinition) -> String {
    let mut statement = String::from("BEGIN");
    match definition.isolation {
        Isolation::Default => {}
        Isolation::ReadUncommitted => statement.push_str(" ISOLATION LEVEL READ UNCOMMITTED"),
        Isolation::ReadCommitted => statement.push_str(" ISOLATION LEVEL READ COMMITTED"),
        Isolation::RepeatableRead => statement.push_str(" ISOLATION LEVEL REPEATABLE READ"),
        Isolation::Serializable => statement.push_str(" ISOLATION LEVEL SERIALIZABLE"),
    }
    if definition.read_only {
        statement.push_str(" READ ONLY");
    }
    statement
}

impl ConnectionBackend for TokioPostgresBackend {
    type Connection = Conn;
    type Error = PoolError;

    const NAME: &'static str = "demarcate-tokio-postgres";

    const UNUSABLE_MESSAGE: &'static str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
         when a statement fails, and the failure was not returned from the transactional function. Return the \
         error, or run the statement that may fail in a nested transaction (propagation = Nested)";

    async fn acquire(&self) -> Result<Conn, PoolError> {
        Ok(Conn { client: Some(self.pool.get().await?), in_transaction: false })
    }

    async fn begin(&self, definition: &TransactionDefinition) -> Result<Conn, PoolError> {
        let connection = Conn { client: Some(self.pool.get().await?), in_transaction: true };
        connection.client().batch_execute(&begin_statement(definition)).await?;
        Ok(connection)
    }

    fn in_transaction(connection: &Conn) -> bool {
        connection.in_transaction
    }

    async fn commit(&self, connection: Conn) -> Result<(), PoolError> {
        connection.finish("COMMIT").await
    }

    async fn rollback(&self, connection: Conn) -> Result<(), PoolError> {
        connection.finish("ROLLBACK").await
    }

    async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), PoolError> {
        Ok(connection.client().batch_execute(statement).await?)
    }

    async fn apply_timeout(&self, connection: &mut Conn, remaining: Duration) -> Result<(), PoolError> {
        let millis = remaining.as_millis().max(1);
        Ok(connection
            .client()
            .batch_execute(&format!("select set_config('statement_timeout', '{millis}', true)"))
            .await?)
    }

    fn taints_transaction(&self, error: &PoolError) -> bool {
        matches!(error, PoolError::Backend(error) if error.as_db_error().is_some())
    }

    async fn is_usable(&self, connection: &mut Conn) -> bool {
        connection.client().batch_execute("select 1").await.is_ok()
    }

    fn release_failure_is_ignorable(&self, error: &PoolError) -> bool {
        matches!(error, PoolError::Backend(error) if error.code().is_some_and(|code| code.code() == "3B001"))
    }
}

/// The connection of one transaction, or of one synchronization scope without a transaction,
/// bound to the transaction context under the key of its pool.
pub type ConnectionHolder = demarcate_connection::ConnectionHolder<TokioPostgresBackend>;

/// A deadpool-postgres pool that hands out the client of the current transaction.
///
/// Inside a transaction [`client`](TransactionalPool::client) returns the client of that
/// transaction. Outside of one it returns a pooled client in autocommit mode, unless the pool
/// was made with [`transaction_required`](TransactionalPool::transaction_required).
///
/// Drop the returned client before the next call of `client` and before calling another
/// transactional function: while it is alive the connection is in use, and a second request for
/// it returns an error.
///
/// A transaction with a timeout sets `statement_timeout` to the remaining time each time a
/// client is handed out. A request after the deadline fails with
/// [`TransactionError::TimedOut`]; a statement that is running at the deadline is cancelled by
/// the server.
///
/// PostgreSQL aborts a transaction when a statement in it fails. The statements run on the
/// client itself, so their failures are not seen here. The commit therefore asks the server
/// whether the transaction is usable and returns [`TransactionError::UnexpectedRollback`] if it
/// is not.
///
/// Do not begin a transaction on the client with `Client::transaction` or with SQL inside a
/// transactional function.
///
/// Cloning is cheap; clones share the pool and the transaction.
#[derive(Clone)]
pub struct TransactionalPool {
    source: ConnectionSource<TokioPostgresBackend>,
}

impl From<Pool> for TransactionalPool {
    fn from(pool: Pool) -> Self {
        Self::new(pool)
    }
}

impl std::fmt::Debug for TransactionalPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalPool")
            .field("status", &self.inner().status())
            .field("key", self.resource_key())
            .field("transaction_required", &self.is_transaction_required())
            .finish()
    }
}

impl TransactionalPool {
    /// Wraps a deadpool-postgres pool.
    ///
    /// Create one `TransactionalPool` for each `Pool` and clone it. Two pools created from the
    /// same `Pool` do not see each other's transactions.
    pub fn new(pool: Pool) -> Self {
        Self { source: ConnectionSource::new(TokioPostgresBackend { pool }) }
    }

    /// Returns a pool that refuses to hand out a client outside a transaction.
    ///
    /// Give this pool to a repository whose functions must always run inside a transaction. A call
    /// without one returns [`TransactionError::NoTransaction`] instead of running in autocommit mode.
    pub fn transaction_required(&self) -> Self {
        Self { source: self.source.transaction_required() }
    }

    /// Returns whether this pool refuses to hand out a client outside a transaction.
    pub fn is_transaction_required(&self) -> bool {
        self.source.is_transaction_required()
    }

    /// Returns the deadpool-postgres pool. A client taken from it does not take part in any
    /// transaction.
    pub fn inner(&self) -> &Pool {
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

    /// Returns the client of the current transaction, or a pooled client when there is none.
    ///
    /// # Errors
    ///
    /// Returns an error when the client of the transaction is already in use, when a nested
    /// transaction of another call owns it, when the transaction that the calling future joined has
    /// completed, or when no connection can be obtained. Returns [`TransactionError::TimedOut`] when
    /// the deadline of the transaction has passed, and [`TransactionError::NoTransaction`] when the
    /// pool requires a transaction and none is running.
    pub async fn client(&self) -> Result<TransactionalClient, TransactionError> {
        let handle = self.source.acquire().await?;
        if let Some(holder) = handle.transaction_holder() {
            holder.mark_aborted();
        }
        Ok(TransactionalClient(handle))
    }
}

/// A client obtained from a [`TransactionalPool`]: the client of the current transaction, or a
/// pooled client.
///
/// It dereferences to [`tokio_postgres::Client`]. Dropping it does not commit or roll back; the
/// transaction manager completes the transaction.
pub struct TransactionalClient(ConnectionHandle<TokioPostgresBackend>);

impl Deref for TransactionalClient {
    type Target = tokio_postgres::Client;
    fn deref(&self) -> &Self::Target {
        self.0.client()
    }
}

#[doc(hidden)]
pub type TokioPostgresTransactionObject = demarcate_connection::ConnectionTransactionObject<TokioPostgresBackend>;

/// The [`TransactionDriver`](demarcate_core::TransactionDriver) for tokio-postgres.
///
/// A nested transaction is a savepoint on the connection of the outer transaction. While it
/// runs, that connection belongs to the nested call, and a future beside it that uses the
/// connection gets an error.
pub type TokioPostgresTransactionDriver = ConnectionDriver<TokioPostgresBackend>;

/// The transaction manager for tokio-postgres.
pub type TokioPostgresTransactionManager = TransactionManager<TokioPostgresTransactionDriver>;

/// Creates a transaction manager for the given pool, with nested transactions allowed.
///
/// Register it once at startup:
/// `demarcate::registry().set_default(Arc::new(manager(pool.clone())))`.
pub fn manager(pool: TransactionalPool) -> TokioPostgresTransactionManager {
    demarcate_connection::manager(pool.source)
}

/// Creates a transaction manager for the given pool with the given settings.
pub fn manager_with_settings(pool: TransactionalPool, settings: ManagerSettings) -> TokioPostgresTransactionManager {
    demarcate_connection::manager_with_settings(pool.source, settings)
}
