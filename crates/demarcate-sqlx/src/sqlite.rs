use std::time::Duration;

use demarcate_core::{Isolation, TransactionDefinition, TransactionError};
use sqlx::{AssertSqlSafe, Executor, Sqlite, SqliteConnection, SqlitePool, Transaction};

const UNUSABLE_MESSAGE: &str = "Transaction rolled back because a statement in it failed and the failure was not \
     returned from the transactional function";

fn validate(definition: &TransactionDefinition) -> Result<(), TransactionError> {
    match definition.isolation {
        Isolation::Default | Isolation::Serializable => Ok(()),
        other => Err(TransactionError::InvalidIsolationLevel(format!(
            "SQLite runs every transaction serializable and does not support the isolation level {other:?}. Use \
             Isolation::Default or Isolation::Serializable"
        ))),
    }
}

async fn begin(
    pool: &SqlitePool,
    _definition: &TransactionDefinition,
) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
    pool.begin().await
}

async fn commit(started: Transaction<'static, Sqlite>) -> Result<(), sqlx::Error> {
    started.commit().await
}

async fn rollback(started: Transaction<'static, Sqlite>) -> Result<(), sqlx::Error> {
    started.rollback().await
}

async fn execute(connection: &mut SqliteConnection, statement: &str) -> Result<(), sqlx::Error> {
    connection.execute(AssertSqlSafe(statement.to_owned())).await.map(|_| ())
}

async fn apply_timeout(_connection: &mut SqliteConnection, _remaining: Duration) -> Result<(), sqlx::Error> {
    Ok(())
}

fn taints_transaction(_error: &sqlx::Error) -> bool {
    false
}

fn ends_transaction(_error: &sqlx::Error) -> bool {
    false
}

async fn is_usable(_connection: &mut SqliteConnection) -> bool {
    true
}

fn release_failure_is_ignorable(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database| database.message().contains("no such savepoint"))
}

sqlx_driver! {
    database: Sqlite,
    pool: SqlitePool,
    connection: SqliteConnection,
    transaction: Transaction<'static, Sqlite>,
    backend: SqliteBackend,
    object: SqliteTransactionObject,
    driver: SqliteTransactionDriver,
    manager: SqliteTransactionManager,
    name: "demarcate-sqlx-sqlite",
    database_name: "SQLite",
    backend_doc: "A transaction begins with `BEGIN`, which is deferred: it takes the write lock at its first write.
SQLite runs every transaction serializable, so only `Isolation::Default` and `Isolation::Serializable` are
accepted. The read-only flag is not enforced: SQLite has no read-only transaction.",
    pool_doc: "A transaction with a timeout checks its deadline whenever a query starts: a query that starts
after the deadline fails with [`TransactionError::TimedOut`]. A
query that is running at the deadline is not interrupted. The commit does not check the deadline.

Note that SQLite allows one writer at a time. While a transaction has written, a write in another
transaction, for example in a `RequiresNew` call inside it, waits for the busy timeout of the
connection and then fails with `database is locked`.",
    acquire_note: "Note that the returned connection is the one that holds the transaction, so its queries run in the
transaction.",
}
