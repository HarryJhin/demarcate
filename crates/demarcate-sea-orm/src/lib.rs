//! Transaction management for SeaORM.
//!
//! [`TransactionalDatabase`] wraps a [`DatabaseConnection`] and implements
//! [`ConnectionTrait`], so that entity operations and statements run on the connection of the
//! current transaction without a transaction argument:
//! `Entity::find().all(&database)`.
//!
//! The database is the one of the wrapped connection: PostgreSQL, MySQL, MariaDB or SQLite.
//! Applications normally depend on the `demarcate` crate with the `sea-orm` feature.

#![warn(missing_docs)]

use std::time::Duration;

use demarcate_connection::{ConnectionBackend, ConnectionDriver, ConnectionHandle, ConnectionSource};
use demarcate_core::{
    Isolation, ManagerSettings, ResourceKey, TransactionDefinition, TransactionError, TransactionManager,
};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, ExecResult, IsolationLevel,
    QueryResult, RuntimeErr, Statement, TransactionTrait, sqlx,
};

pub use demarcate_connection::CONNECTION_SYNCHRONIZATION_ORDER;

#[doc(hidden)]
pub enum Conn {
    Plain(DatabaseConnection),
    Transaction(DatabaseTransaction),
}

impl Conn {
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        match self {
            Self::Plain(connection) => connection.execute_unprepared(sql).await,
            Self::Transaction(transaction) => transaction.execute_unprepared(sql).await,
        }
    }
}

/// The [`ConnectionBackend`] for SeaORM.
///
/// The isolation level and the read-only flag are passed to SeaORM when the transaction begins.
/// On SQLite only `Isolation::Default` and `Isolation::Serializable` are accepted, and the
/// read-only flag is not enforced.
///
/// On a Tokio runtime the transaction begins in a task of its own, and on MySQL and MariaDB every
/// statement runs in a task of its own. A call that is cancelled therefore cannot leave an open
/// transaction or a half-read statement on a pooled connection. Note that on another runtime this
/// protection is missing for MySQL and MariaDB.
pub struct SeaOrmBackend {
    connection: DatabaseConnection,
}

fn database_error(error: &DbErr) -> Option<&sqlx::Error> {
    match error {
        DbErr::Conn(RuntimeErr::SqlxError(error))
        | DbErr::Exec(RuntimeErr::SqlxError(error))
        | DbErr::Query(RuntimeErr::SqlxError(error)) => Some(error),
        _ => None,
    }
}

fn sqlstate(error: &DbErr) -> Option<String> {
    database_error(error)?.as_database_error()?.code().map(|code| code.into_owned())
}

impl ConnectionBackend for SeaOrmBackend {
    type Connection = Conn;
    type Error = DbErr;

    const NAME: &'static str = "demarcate-sea-orm";

    const UNUSABLE_MESSAGE: &'static str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
         when a statement fails, and the failure was not returned from the transactional function. Return the \
         error, or run the statement that may fail in a nested transaction (propagation = Nested)";

    fn validate(&self, definition: &TransactionDefinition) -> Result<(), TransactionError> {
        match (self.connection.get_database_backend(), definition.isolation) {
            (DbBackend::Sqlite, Isolation::Default | Isolation::Serializable) => Ok(()),
            (DbBackend::Sqlite, other) => Err(TransactionError::InvalidIsolationLevel(format!(
                "SQLite runs every transaction serializable and does not support the isolation level {other:?}. Use \
                 Isolation::Default or Isolation::Serializable"
            ))),
            _ => Ok(()),
        }
    }

    async fn acquire(&self) -> Result<Conn, DbErr> {
        Ok(Conn::Plain(self.connection.clone()))
    }

    async fn begin(&self, definition: &TransactionDefinition) -> Result<Conn, DbErr> {
        let connection = self.connection.clone();
        let sqlite = connection.get_database_backend() == DbBackend::Sqlite;
        let isolation = match definition.isolation {
            Isolation::Default => None,
            Isolation::ReadUncommitted => Some(IsolationLevel::ReadUncommitted),
            Isolation::ReadCommitted => Some(IsolationLevel::ReadCommitted),
            Isolation::RepeatableRead => Some(IsolationLevel::RepeatableRead),
            Isolation::Serializable => Some(IsolationLevel::Serializable),
        };
        let access = definition.read_only.then_some(AccessMode::ReadOnly);
        let begin = async move {
            if sqlite { connection.begin().await } else { connection.begin_with_config(isolation, access).await }
        };
        // A begin that is cancelled half-way can leave the transaction open on a pooled
        // connection. In a task of its own the begin always runs to its end; when the caller is
        // gone, the transaction is dropped there and rolled back.
        let transaction = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn(begin)
                .await
                .map_err(|error| DbErr::Custom(format!("The task that began the transaction failed: {error}")))??,
            Err(_) => begin.await?,
        };
        Ok(Conn::Transaction(transaction))
    }

    fn in_transaction(connection: &Conn) -> bool {
        matches!(connection, Conn::Transaction(_))
    }

    async fn commit(&self, connection: Conn) -> Result<(), DbErr> {
        match connection {
            Conn::Transaction(transaction) => transaction.commit().await,
            Conn::Plain(_) => Ok(()),
        }
    }

    async fn rollback(&self, connection: Conn) -> Result<(), DbErr> {
        match connection {
            Conn::Transaction(transaction) => transaction.rollback().await,
            Conn::Plain(_) => Ok(()),
        }
    }

    async fn execute(&self, connection: &mut Conn, statement: &str) -> Result<(), DbErr> {
        connection.execute_unprepared(statement).await.map(|_| ())
    }

    async fn apply_timeout(&self, connection: &mut Conn, remaining: Duration) -> Result<(), DbErr> {
        if self.connection.get_database_backend() != DbBackend::Postgres {
            return Ok(());
        }
        let millis = remaining.as_millis().max(1);
        connection
            .execute_unprepared(&format!("select set_config('statement_timeout', '{millis}', true)"))
            .await
            .map(|_| ())
    }

    fn taints_transaction(&self, error: &DbErr) -> bool {
        self.connection.get_database_backend() == DbBackend::Postgres
            && database_error(error).is_some_and(|error| error.as_database_error().is_some())
    }

    fn ends_transaction(&self, error: &DbErr) -> bool {
        self.connection.get_database_backend() == DbBackend::MySql
            && (sqlstate(error).is_some_and(|code| code == "40001")
                || database_error(error)
                    .and_then(|error| error.as_database_error())
                    .is_some_and(|database| database.message().contains("Record has changed since last read")))
    }

    async fn is_usable(&self, connection: &mut Conn) -> bool {
        self.connection.get_database_backend() != DbBackend::Postgres
            || connection.execute_unprepared("select 1").await.is_ok()
    }

    fn release_failure_is_ignorable(&self, error: &DbErr) -> bool {
        let Some(database) = database_error(error).and_then(|error| error.as_database_error()) else {
            return false;
        };
        let code = database.code();
        match self.connection.get_database_backend() {
            DbBackend::Postgres => code.is_some_and(|code| code == "3B001"),
            DbBackend::MySql => {
                code.is_some_and(|code| code == "42000") && database.message().contains("does not exist")
            }
            DbBackend::Sqlite => database.message().contains("no such savepoint"),
            _ => false,
        }
    }
}

/// The connection of one transaction, or of one synchronization scope without a transaction,
/// bound to the transaction context under the key of its [`TransactionalDatabase`].
pub type ConnectionHolder = demarcate_connection::ConnectionHolder<SeaOrmBackend>;

/// A SeaORM connection that runs every query on the connection of the current transaction.
///
/// Pass a reference to it wherever SeaORM takes a [`ConnectionTrait`]:
/// `Entity::find().all(&database)`, `model.insert(&database)`. Inside a transaction the query
/// runs in that transaction. Outside of one it runs on the wrapped connection in autocommit
/// mode, unless this value was made with
/// [`transaction_required`](TransactionalDatabase::transaction_required).
///
/// The queries of one transaction must be awaited one after another. Awaiting two of them at
/// once returns an error; it does not open a second connection.
///
/// A transaction with a timeout checks its deadline whenever a query starts: a query that starts
/// after the deadline fails with [`TransactionError::TimedOut`]. On PostgreSQL the server also
/// cancels a statement that is running at the deadline.
///
/// On PostgreSQL a statement that fails inside a transaction is recorded, and the commit returns
/// [`TransactionError::UnexpectedRollback`] if the server has aborted the transaction. On MySQL
/// and MariaDB a deadlock (SQLSTATE `40001`) ends the transaction on the server; every query
/// that follows in that transaction fails with an error instead of running outside it.
///
/// An error of the transaction infrastructure raised during a query arrives as
/// `DbErr::Conn(RuntimeErr::SqlxError(_))` that holds a `sqlx::Error::Configuration` with the
/// [`TransactionError`]; [`transaction_error`] returns it.
///
/// This type implements [`ConnectionTrait`] only. It does not implement SeaORM's `StreamTrait`
/// and `TransactionTrait`, so `stream` and `transaction` cannot be called on it.
///
/// Do not begin a transaction on the wrapped connection with SeaORM's own `begin` or
/// `transaction` inside a transactional function: that transaction is a separate one.
///
/// Cloning is cheap; clones share the connection and the transaction.
#[derive(Clone, Debug)]
pub struct TransactionalDatabase {
    source: ConnectionSource<SeaOrmBackend>,
    backend: DbBackend,
}

/// The error of [`TransactionalDatabase::connect`]: the message of the SeaORM error, without the
/// URL of the database.
///
/// SeaORM puts the URL into the message of some connection errors, and a URL can contain a
/// password. The message can still name the user and the database, as the error of the server
/// does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectError {
    message: String,
}

impl ConnectError {
    fn new(error: &DbErr, url: &str) -> Self {
        let message = error.to_string();
        Self { message: if url.is_empty() { message } else { message.replace(url, "<database url>") } }
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConnectError {}

impl From<DatabaseConnection> for TransactionalDatabase {
    fn from(connection: DatabaseConnection) -> Self {
        Self::new(connection)
    }
}

/// Returns the error of the transaction infrastructure that a query through
/// [`TransactionalDatabase`] failed with, or `None` when the error has another cause.
pub fn transaction_error(error: &DbErr) -> Option<&TransactionError> {
    match error {
        DbErr::Conn(RuntimeErr::SqlxError(error)) => match &**error {
            sqlx::Error::Configuration(error) => error.downcast_ref::<TransactionError>(),
            _ => None,
        },
        _ => None,
    }
}

fn into_db_err(error: TransactionError) -> DbErr {
    DbErr::Conn(RuntimeErr::SqlxError(std::sync::Arc::new(sqlx::Error::Configuration(Box::new(error)))))
}

impl TransactionalDatabase {
    /// Wraps a SeaORM connection.
    ///
    /// Create one `TransactionalDatabase` for each `DatabaseConnection` and clone it. Two values
    /// created from the same connection do not see each other's transactions.
    pub fn new(connection: DatabaseConnection) -> Self {
        let backend = connection.get_database_backend();
        Self { source: ConnectionSource::new(SeaOrmBackend { connection }), backend }
    }

    /// Connects a SeaORM connection with the given settings and wraps it.
    ///
    /// A setting that is not given keeps the default of SeaORM. The transaction timeout of the
    /// settings belongs to the transaction manager and is not used here.
    ///
    /// SeaORM records its connect options, with the URL, in a `tracing` span at the trace level.
    /// Do not collect the trace level of `sea_orm` where the URL must stay secret.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL is not valid or the database cannot be reached.
    pub async fn connect(settings: &demarcate_connection::PoolSettings) -> Result<Self, ConnectError> {
        let mut options = sea_orm::ConnectOptions::new(settings.url().to_owned());
        if let Some(max_connections) = settings.get_max_connections() {
            options.max_connections(max_connections);
        }
        if let Some(min_connections) = settings.get_min_connections() {
            options.min_connections(min_connections);
        }
        if let Some(acquire_timeout) = settings.get_acquire_timeout() {
            options.acquire_timeout(acquire_timeout);
        }
        if let Some(idle_timeout) = settings.get_idle_timeout() {
            options.idle_timeout(idle_timeout);
        }
        match sea_orm::Database::connect(options).await {
            Ok(connection) => Ok(Self::new(connection)),
            Err(error) => Err(ConnectError::new(&error, settings.url())),
        }
    }

    /// Returns a connection that refuses to run a query outside a transaction.
    ///
    /// Give it to a repository whose functions must always run inside a transaction. A call
    /// without one returns [`TransactionError::NoTransaction`] instead of running in autocommit mode.
    pub fn transaction_required(&self) -> Self {
        Self { source: self.source.transaction_required(), backend: self.backend }
    }

    /// Returns whether this connection refuses to run a query outside a transaction.
    pub fn is_transaction_required(&self) -> bool {
        self.source.is_transaction_required()
    }

    /// Returns the SeaORM connection. A query on it does not take part in any transaction.
    pub fn inner(&self) -> &DatabaseConnection {
        &self.source.backend().connection
    }

    /// Returns the key under which the connection of a transaction is bound to the context.
    pub fn resource_key(&self) -> &ResourceKey {
        self.source.resource_key()
    }

    /// Returns whether a connection of this value is bound to the context of the calling future.
    pub fn is_connection_transactional(&self) -> bool {
        self.source.is_connection_transactional()
    }

    async fn run(&self, operation: Operation) -> Result<Output, DbErr> {
        let connection = self.source.acquire().await.map_err(into_db_err)?;
        // sqlx cannot use a MySQL connection again when a statement on it was cancelled half-way.
        // In a task of its own the statement always runs to its end, also when the caller is gone.
        if self.backend == DbBackend::MySql
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            return runtime
                .spawn(operation.run(connection))
                .await
                .map_err(|error| DbErr::Custom(format!("The task that ran the statement failed: {error}")))?;
        }
        operation.run(connection).await
    }
}

enum Operation {
    Execute(Statement),
    ExecuteUnprepared(String),
    QueryOne(Statement),
    QueryAll(Statement),
}

enum Output {
    Executed(ExecResult),
    One(Option<QueryResult>),
    All(Vec<QueryResult>),
}

impl Operation {
    async fn run(self, connection: ConnectionHandle<SeaOrmBackend>) -> Result<Output, DbErr> {
        let result = match (&*connection, self) {
            (Conn::Plain(plain), Self::Execute(statement)) => plain.execute_raw(statement).await.map(Output::Executed),
            (Conn::Plain(plain), Self::ExecuteUnprepared(sql)) => {
                plain.execute_unprepared(&sql).await.map(Output::Executed)
            }
            (Conn::Plain(plain), Self::QueryOne(statement)) => plain.query_one_raw(statement).await.map(Output::One),
            (Conn::Plain(plain), Self::QueryAll(statement)) => plain.query_all_raw(statement).await.map(Output::All),
            (Conn::Transaction(transaction), Self::Execute(statement)) => {
                transaction.execute_raw(statement).await.map(Output::Executed)
            }
            (Conn::Transaction(transaction), Self::ExecuteUnprepared(sql)) => {
                transaction.execute_unprepared(&sql).await.map(Output::Executed)
            }
            (Conn::Transaction(transaction), Self::QueryOne(statement)) => {
                transaction.query_one_raw(statement).await.map(Output::One)
            }
            (Conn::Transaction(transaction), Self::QueryAll(statement)) => {
                transaction.query_all_raw(statement).await.map(Output::All)
            }
        };
        connection.statement_finished(result.as_ref().err());
        result
    }
}

fn unexpected_output() -> DbErr {
    DbErr::Custom("The statement returned a result of another kind".into())
}

#[async_trait::async_trait]
impl ConnectionTrait for TransactionalDatabase {
    fn get_database_backend(&self) -> DbBackend {
        self.backend
    }

    async fn execute_raw(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        match self.run(Operation::Execute(stmt)).await? {
            Output::Executed(result) => Ok(result),
            _ => Err(unexpected_output()),
        }
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        match self.run(Operation::ExecuteUnprepared(sql.to_owned())).await? {
            Output::Executed(result) => Ok(result),
            _ => Err(unexpected_output()),
        }
    }

    async fn query_one_raw(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        match self.run(Operation::QueryOne(stmt)).await? {
            Output::One(result) => Ok(result),
            _ => Err(unexpected_output()),
        }
    }

    async fn query_all_raw(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match self.run(Operation::QueryAll(stmt)).await? {
            Output::All(result) => Ok(result),
            _ => Err(unexpected_output()),
        }
    }

    fn support_returning(&self) -> bool {
        self.inner().support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.inner().is_mock_connection()
    }
}

#[doc(hidden)]
pub type SeaOrmTransactionObject = demarcate_connection::ConnectionTransactionObject<SeaOrmBackend>;

/// The [`TransactionDriver`](demarcate_core::TransactionDriver) for SeaORM.
///
/// A nested transaction is a savepoint on the connection of the outer transaction. While it
/// runs, that connection belongs to the nested call, and a future beside it that uses the
/// connection gets an error.
pub type SeaOrmTransactionDriver = ConnectionDriver<SeaOrmBackend>;

/// The transaction manager for SeaORM.
pub type SeaOrmTransactionManager = TransactionManager<SeaOrmTransactionDriver>;

/// Creates a transaction manager for the given connection, with nested transactions allowed.
///
/// The `install` function of this module in the `demarcate` crate creates and registers it in
/// one call. Call this function to register the manager yourself:
/// `demarcate::registry().set_default(Arc::new(manager(database.clone())))`.
pub fn manager(database: TransactionalDatabase) -> SeaOrmTransactionManager {
    demarcate_connection::manager(database.source)
}

/// Creates a transaction manager for the given connection with the given settings.
pub fn manager_with_settings(database: TransactionalDatabase, settings: ManagerSettings) -> SeaOrmTransactionManager {
    demarcate_connection::manager_with_settings(database.source, settings)
}
