use std::time::Duration;

use demarcate_core::{Isolation, TransactionDefinition, TransactionError};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres, Transaction};

const UNUSABLE_MESSAGE: &str = "Transaction rolled back because a statement in it failed. PostgreSQL aborts a transaction \
                     when a statement fails, and the failure was not returned from the transactional function. \
                     Return the error, or run the statement that may fail in a nested transaction \
                     (propagation = Nested)";

fn validate(_definition: &TransactionDefinition) -> Result<(), TransactionError> {
    Ok(())
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

async fn begin(
    pool: &PgPool,
    definition: &TransactionDefinition,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    pool.begin_with(AssertSqlSafe(begin_statement(definition))).await
}

async fn execute(connection: &mut PgConnection, statement: &str) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(statement.to_owned())).execute(connection).await.map(|_| ())
}

async fn apply_timeout(connection: &mut PgConnection, remaining: Duration) -> Result<(), sqlx::Error> {
    let millis = remaining.as_millis().max(1).to_string();
    sqlx::query("select set_config('statement_timeout', $1, true)").bind(millis).execute(connection).await.map(|_| ())
}

fn taints_transaction(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some()
}

fn ends_transaction(_error: &sqlx::Error) -> bool {
    false
}

async fn is_usable(connection: &mut PgConnection) -> bool {
    sqlx::query("select 1").execute(connection).await.is_ok()
}

fn release_failure_is_ignorable(error: &sqlx::Error) -> bool {
    let code_matches =
        error.as_database_error().and_then(|database| database.code()).is_some_and(|code| code == "3B001");
    let message = error.to_string();
    code_matches || message.contains("not supported") || message.contains("3B001")
}

sqlx_driver! {
    database: Postgres,
    pool: PgPool,
    connection: PgConnection,
    backend: PgBackend,
    object: PgTransactionObject,
    driver: PgTransactionDriver,
    manager: PgTransactionManager,
    name: "demarcate-sqlx-postgres",
    database_name: "PostgreSQL",
    backend_doc: "A read-only transaction begins as `READ ONLY`, so PostgreSQL rejects a write in it.",
    pool_doc: "A transaction with a timeout sets `statement_timeout` to the remaining time before every
query. A query that starts after the deadline fails with
[`TransactionError::TimedOut`]; a query that is running at the
deadline is cancelled by the server and fails with a database error. The commit does not check the
deadline.

A statement that fails inside a transaction is recorded. When the transactional function then
returns `Ok`, the commit checks with the server and returns
[`TransactionError::UnexpectedRollback`] if PostgreSQL
has aborted the transaction.",
    acquire_note: "Note that a statement that fails on the returned connection is not recorded, so the commit
does not detect that PostgreSQL aborted the transaction.",
}
