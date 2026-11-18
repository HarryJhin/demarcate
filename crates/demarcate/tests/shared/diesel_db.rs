// The data access of the conformance tests for a diesel-async driver. Included inside
// `mod db`, which defines `Connection`, `placeholders` and the driver module `driver`.

use std::fmt;

use demarcate::TransactionError;
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

pub use driver::{TransactionalPool, manager};

pub type Raw = Pool<Connection>;

#[derive(Debug)]
pub enum Error {
    Transaction(TransactionError),
    Diesel(diesel::result::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transaction(error) => write!(f, "{error}"),
            Self::Diesel(error) => write!(f, "{error}"),
        }
    }
}

#[derive(diesel::QueryableByName)]
struct Value {
    #[diesel(sql_type = Text)]
    v: String,
}

pub fn pool(url: &str, connections: u32) -> Raw {
    Pool::builder(AsyncDieselConnectionManager::<Connection>::new(url)).max_size(connections as usize).build().expect("pool")
}

pub fn transactional(raw: &Raw) -> TransactionalPool {
    TransactionalPool::new(raw.clone())
}

pub async fn raw_execute(raw: &Raw, sql: &str) {
    raw.get().await.unwrap().batch_execute(sql).await.unwrap();
}

pub async fn raw_values(raw: &Raw, sql: &str) -> Vec<String> {
    let sql = sql.replacen("select ", "select (", 1).replacen(" from ", ") as v from ", 1);
    let rows: Vec<Value> = diesel::sql_query(sql).load(&mut *raw.get().await.unwrap()).await.unwrap();
    rows.into_iter().map(|row| row.v).collect()
}

pub async fn insert(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
    let mut connection = pool.connection().await.map_err(Error::Transaction)?;
    diesel::sql_query(placeholders(&format!("insert into {table} values($1)")))
        .bind::<Text, _>(value)
        .execute(&mut *connection)
        .await
        .map_err(Error::Diesel)?;
    Ok(())
}

pub async fn values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    let mut connection = pool.connection().await.map_err(Error::Transaction)?;
    let rows: Vec<Value> = diesel::sql_query(format!("select v from {table} order by v"))
        .load(&mut *connection)
        .await
        .map_err(Error::Diesel)?;
    Ok(rows.into_iter().map(|row| row.v).collect())
}

pub async fn stream_values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
    values(pool, table).await
}

pub fn transaction_error(error: &Error) -> Option<&TransactionError> {
    match error {
        Error::Transaction(error) => Some(error),
        Error::Diesel(_) => None,
    }
}

pub fn is_database_error(error: &Error) -> bool {
    matches!(error, Error::Diesel(diesel::result::Error::DatabaseError(..)))
}
