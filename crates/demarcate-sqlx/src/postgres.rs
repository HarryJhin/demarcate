use std::ops::{Deref, DerefMut};
use std::time::Duration;

use demarcate_connection::{ConnectionBackend, ConnectionDriver, ConnectionHandle, ConnectionSource};
use demarcate_core::{
    Isolation, ManagerSettings, ResourceKey, TransactionDefinition, TransactionError, TransactionManager,
};
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres, Transaction};

pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

#[doc(hidden)]
pub enum Conn {
    Pooled(PoolConnection<Postgres>),
    Transaction(Transaction<'static, Postgres>),
}

impl Conn {
    fn connection(&mut self) -> &mut PgConnection {
        match self {
            Self::Pooled(connection) => connection,
            Self::Transaction(transaction) => transaction,
        }
    }
    fn connection_ref(&self) -> &PgConnection {
        match self {
            Self::Pooled(connection) => connection,
            Self::Transaction(transaction) => transaction,
        }
    }
}

/// The [`ConnectionBackend`] for PostgreSQL through sqlx.
///
/// A read-only transaction begins as `READ ONLY`, so PostgreSQL rejects a write in it.
pub struct PgBackend {
    pool: PgPool,
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

impl ConnectionBackend for PgBackend {
    type Connection = Conn;
    type Error = sqlx::Error;

    const NAME: &'static str = "demarcate-sqlx-postgres";

    const UNUSABLE_MESSAGE: &'static str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
                     when a statement fails, and the failure was not returned from the transactional function. \
                     Return the error, or run the statement that may fail in a nested transaction \
                     (propagation = Nested)";

    async fn acquire(&self) -> Result<Conn, sqlx::Error> {
        Ok(Conn::Pooled(self.pool.acquire().await?))
    }

    async fn begin(&self, definition: &TransactionDefinition) -> Result<Conn, sqlx::Error> {
        Ok(Conn::Transaction(self.pool.begin_with(AssertSqlSafe(begin_statement(definition))).await?))
    }

    fn in_transaction(connection: &Conn) -> bool {
        matches!(connection, Conn::Transaction(_))
    }

    async fn commit(&self, connection: Conn) -> Result<(), sqlx::Error> {
        match connection {
            Conn::Transaction(started) => started.commit().await,
            Conn::Pooled(_) => Ok(()),
        }
    }

    async fn rollback(&self, connection: Conn) -> Result<(), sqlx::Error> {
        match connection {
            Conn::Transaction(started) => started.rollback().await,
            Conn::Pooled(_) => Ok(()),
        }
    }

    async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), sqlx::Error> {
        sqlx::query(AssertSqlSafe(statement.to_owned())).execute(connection.connection()).await.map(|_| ())
    }

    async fn apply_timeout(&self, connection: &mut Conn, remaining: Duration) -> Result<(), sqlx::Error> {
        let millis = remaining.as_millis().max(1).to_string();
        sqlx::query("select set_config('statement_timeout', $1, true)")
            .bind(millis)
            .execute(connection.connection())
            .await
            .map(|_| ())
    }

    fn taints_transaction(error: &sqlx::Error) -> bool {
        error.as_database_error().is_some()
    }

    async fn is_usable(&self, connection: &mut Conn) -> bool {
        sqlx::query("select 1").execute(connection.connection()).await.is_ok()
    }

    fn release_failure_is_ignorable(error: &sqlx::Error) -> bool {
        let code_matches =
            error.as_database_error().and_then(|database| database.code()).is_some_and(|code| code == "3B001");
        let message = error.to_string();
        code_matches || message.contains("not supported") || message.contains("3B001")
    }
}

/// The connection of one transaction, or of one synchronization scope without a transaction,
/// bound to the transaction context under the key of its pool.
pub type ConnectionHolder = demarcate_connection::ConnectionHolder<PgBackend>;

/// A PostgreSQL pool that runs every query on the connection of the current transaction.
///
/// Pass a reference to it wherever sqlx takes an executor:
/// `sqlx::query("...").execute(&pool)`. Inside a transaction the query runs on the connection of
/// that transaction. Outside of one it runs on a pooled connection in autocommit mode, unless the
/// pool was made with [`transaction_required`](TransactionalPool::transaction_required).
///
/// The queries of one transaction must be awaited one after another. Awaiting two of them at
/// once returns an error; it does not open a second connection.
///
/// A transaction with a timeout sets `statement_timeout` to the remaining time before every
/// query. A query that starts after the deadline fails with [`TransactionError::TimedOut`]; a
/// query that is running at the deadline is cancelled by the server and fails with a database
/// error. The commit does not check the deadline.
///
/// A statement that fails inside a transaction is recorded. When the transactional function then
/// returns `Ok`, the commit checks with the server and returns
/// [`TransactionError::UnexpectedRollback`] if PostgreSQL has aborted the transaction.
///
/// An error of the transaction infrastructure raised during a query arrives as
/// [`sqlx::Error::Configuration`] that wraps the [`TransactionError`].
///
/// Cloning is cheap; clones share the pool and the transaction.
#[derive(Clone, Debug)]
pub struct TransactionalPool {
    source: ConnectionSource<PgBackend>,
}

impl TransactionalPool {
    /// Wraps a sqlx pool.
    ///
    /// Create one `TransactionalPool` for each `PgPool` and clone it. Two pools created from the
    /// same `PgPool` do not see each other's transactions.
    pub fn new(pool: PgPool) -> Self {
        Self { source: ConnectionSource::new(PgBackend { pool }) }
    }

    /// Returns a pool that refuses to run a query outside a transaction.
    ///
    /// Give this pool to a repository whose functions must always run inside a transaction. A call
    /// without one returns [`TransactionError::NoTransaction`] instead of running in autocommit mode.
    pub fn transaction_required(&self) -> Self {
        Self { source: self.source.transaction_required() }
    }

    /// Returns whether this pool refuses to run a query outside a transaction.
    pub fn is_transaction_required(&self) -> bool {
        self.source.is_transaction_required()
    }

    /// Returns the sqlx pool. A query on it does not take part in any transaction.
    pub fn inner(&self) -> &PgPool {
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

    /// Returns the connection of the current transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::NoTransaction`] when the calling future runs in no transaction of
    /// this pool, and otherwise the errors of [`acquire`](TransactionalPool::acquire).
    pub async fn acquire_in_transaction(&self) -> Result<TransactionalConnection, TransactionError> {
        self.source.acquire_in_transaction().await.map(TransactionalConnection)
    }

    /// Returns the connection of the current transaction, or a pooled connection when there is none.
    ///
    /// Prefer passing the pool as the executor. Use this for an API that needs a
    /// `&mut PgConnection`. Drop the returned connection before the next query through the pool and
    /// before calling another transactional function; while it is alive the connection is in use.
    ///
    /// Note that a statement that fails on the returned connection is not recorded, so the commit
    /// does not detect that PostgreSQL aborted the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection of the transaction is already in use, when a nested
    /// transaction of another call owns it, when the transaction that the calling future joined has
    /// completed, or when no connection can be obtained. Returns [`TransactionError::TimedOut`] when
    /// the deadline of the transaction has passed, and [`TransactionError::NoTransaction`] when the
    /// pool requires a transaction and none is running.
    pub async fn acquire(&self) -> Result<TransactionalConnection, TransactionError> {
        self.source.acquire().await.map(TransactionalConnection)
    }
}

fn into_sqlx(error: TransactionError) -> sqlx::Error {
    sqlx::Error::Configuration(Box::new(error))
}

impl<'p> sqlx::Executor<'p> for &'_ TransactionalPool {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<sqlx::Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>, sqlx::Error>,
    >
    where
        'p: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        use futures_util::TryStreamExt;
        let pool = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            let transaction = connection.0.transaction_holder();
            let mut rows = (&mut *connection).fetch_many(query);
            loop {
                match rows.try_next().await {
                    Ok(Some(step)) => yield step,
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        if let Some(holder) = &transaction
                            && PgBackend::taints_transaction(&error)
                        {
                            holder.mark_aborted();
                        }
                        Err(error)?;
                    }
                }
            }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::future::BoxFuture<'e, Result<Option<sqlx::postgres::PgRow>, sqlx::Error>>
    where
        'p: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            let result = (&mut *connection).fetch_optional(query).await;
            connection.0.statement_finished(result.as_ref().err());
            result
        })
    }

    fn prepare_with<'e>(
        self,
        sql: sqlx::SqlStr,
        parameters: &'e [sqlx::postgres::PgTypeInfo],
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::postgres::PgStatement, sqlx::Error>>
    where
        'p: 'e,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            let result = (&mut *connection).prepare_with(sql, parameters).await;
            connection.0.statement_finished(result.as_ref().err());
            result
        })
    }

    fn describe<'e>(
        self,
        sql: sqlx::SqlStr,
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<Postgres>, sqlx::Error>>
    where
        'p: 'e,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            let result = (&mut *connection).describe(sql).await;
            connection.0.statement_finished(result.as_ref().err());
            result
        })
    }
}

/// A connection obtained from a [`TransactionalPool`]: the connection of the current transaction,
/// or a pooled connection.
///
/// It dereferences to [`PgConnection`]. Dropping it does not commit or roll back; the transaction
/// manager completes the transaction.
pub struct TransactionalConnection(ConnectionHandle<PgBackend>);

impl Deref for TransactionalConnection {
    type Target = PgConnection;
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
pub type PgTransactionObject = demarcate_connection::ConnectionTransactionObject<PgBackend>;

/// The [`TransactionDriver`](demarcate_core::TransactionDriver) for PostgreSQL through sqlx.
///
/// A nested transaction is a savepoint on the connection of the outer transaction. While it
/// runs, that connection belongs to the nested call, and a future beside it that uses the
/// connection gets an error.
pub type PgTransactionDriver = ConnectionDriver<PgBackend>;

/// The transaction manager for PostgreSQL through sqlx.
pub type PgTransactionManager = TransactionManager<PgTransactionDriver>;

/// Creates a transaction manager for the given pool, with nested transactions allowed.
///
/// Register it once at startup:
/// `demarcate::registry().set_default(Arc::new(manager(pool.clone())))`.
pub fn manager(pool: TransactionalPool) -> PgTransactionManager {
    demarcate_connection::manager(pool.source)
}

/// Creates a transaction manager for the given pool with the given settings.
pub fn manager_with_settings(pool: TransactionalPool, settings: ManagerSettings) -> PgTransactionManager {
    demarcate_connection::manager_with_settings(pool.source, settings)
}
