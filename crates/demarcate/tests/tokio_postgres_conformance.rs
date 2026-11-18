mod db {
    use std::fmt;
    use std::str::FromStr;

    use deadpool_postgres::{Manager, Pool};
    use demarcate::TransactionError;

    pub use demarcate::tokio_postgres::{TransactionalPool, manager};

    pub const KIND: super::Kind = super::Kind::Postgres;

    pub type Raw = Pool;

    #[derive(Debug)]
    pub enum Error {
        Transaction(TransactionError),
        Postgres(tokio_postgres::Error),
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Transaction(error) => write!(f, "{error}"),
                Self::Postgres(error) => write!(f, "{error}"),
            }
        }
    }

    pub async fn connect(connections: u32) -> Raw {
        let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
        let config = tokio_postgres::Config::from_str(&url).expect("url");
        Pool::builder(Manager::new(config, tokio_postgres::NoTls)).max_size(connections as usize).build().expect("pool")
    }

    pub fn transactional(raw: &Raw) -> TransactionalPool {
        TransactionalPool::new(raw.clone())
    }

    pub async fn raw_execute(raw: &Raw, sql: &str) {
        raw.get().await.unwrap().batch_execute(sql).await.unwrap();
    }

    pub async fn raw_values(raw: &Raw, sql: &str) -> Vec<String> {
        let rows = raw.get().await.unwrap().query(sql, &[]).await.unwrap();
        rows.iter().map(|row| row.get::<_, String>(0)).collect()
    }

    pub async fn insert(pool: &TransactionalPool, table: &str, value: &str) -> Result<(), Error> {
        let client = pool.client().await.map_err(Error::Transaction)?;
        client.execute(&format!("insert into {table} values($1)"), &[&value]).await.map_err(Error::Postgres)?;
        Ok(())
    }

    pub async fn values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
        let client = pool.client().await.map_err(Error::Transaction)?;
        let rows = client.query(&format!("select v from {table} order by v"), &[]).await.map_err(Error::Postgres)?;
        Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
    }

    pub async fn stream_values(pool: &TransactionalPool, table: &str) -> Result<Vec<String>, Error> {
        values(pool, table).await
    }

    pub fn transaction_error(error: &Error) -> Option<&TransactionError> {
        match error {
            Error::Transaction(error) => Some(error),
            Error::Postgres(_) => None,
        }
    }

    pub fn is_database_error(error: &Error) -> bool {
        matches!(error, Error::Postgres(error) if error.as_db_error().is_some())
    }
}

macro_rules! db_test {
    ($name:ident, |$service:ident| $body:block) => {
        #[test]
        #[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
        fn $name() {
            run_test(|$service| Box::pin(async move $body));
        }
    };
}

include!("shared/conformance.rs");
