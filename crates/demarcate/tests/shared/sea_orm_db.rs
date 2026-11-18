// The data access of the conformance tests for the SeaORM driver. Included inside `mod db`,
// which defines `placeholders`.

use demarcate::TransactionError;
use sea_orm::{ConnectionTrait, DbErr, RuntimeErr, Statement};

pub use demarcate::sea_orm::{TransactionalDatabase as TransactionalPool, manager};

pub type Raw = sea_orm::DatabaseConnection;
pub type Error = DbErr;

pub fn transactional(raw: &Raw) -> TransactionalPool {
    TransactionalPool::new(raw.clone())
}

pub async fn raw_execute(raw: &Raw, sql: &str) {
    raw.execute_unprepared(sql).await.unwrap();
}

fn first_column(rows: Vec<sea_orm::QueryResult>) -> Vec<String> {
    rows.iter().map(|row| row.try_get_by_index::<String>(0).unwrap()).collect()
}

pub async fn raw_values(raw: &Raw, sql: &str) -> Vec<String> {
    first_column(raw.query_all_raw(Statement::from_string(raw.get_database_backend(), sql)).await.unwrap())
}

pub async fn insert(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
    let sql = placeholders(&format!("insert into {table} values($1)"));
    pool.execute_raw(Statement::from_sql_and_values(pool.get_database_backend(), sql, [value.into()])).await.map(|_| ())
}

pub async fn values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    let sql = format!("select v from {table} order by v");
    Ok(first_column(pool.query_all_raw(Statement::from_string(pool.get_database_backend(), sql)).await?))
}

pub async fn stream_values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    values(pool, table).await
}

#[allow(dead_code)]
pub async fn increment_marker(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
    let sql = placeholders(&format!("update {table} set marker = marker + 1 where v = $1"));
    pool.execute_raw(Statement::from_sql_and_values(pool.get_database_backend(), sql, [value.into()])).await.map(|_| ())
}

pub fn transaction_error(error: &Error) -> Option<&TransactionError> {
    demarcate::sea_orm::transaction_error(error)
}

fn database_error(error: &Error) -> Option<&dyn sqlx::error::DatabaseError> {
    match error {
        DbErr::Exec(RuntimeErr::SqlxError(error)) | DbErr::Query(RuntimeErr::SqlxError(error)) => {
            error.as_database_error()
        }
        _ => None,
    }
}

pub fn is_database_error(error: &Error) -> bool {
    database_error(error).is_some()
}

#[allow(dead_code)]
pub fn sqlstate(error: &Error) -> Option<String> {
    database_error(error).and_then(|database| database.code()).map(|code| code.into_owned())
}
