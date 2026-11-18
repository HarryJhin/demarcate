mod db {
    pub use demarcate::postgres::{TransactionalPool, manager};

    pub const KIND: super::Kind = super::Kind::Postgres;

    pub fn placeholders(sql: &str) -> String {
        sql.to_owned()
    }

    pub type Raw = sqlx::PgPool;

    include!("shared/sqlx_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
        sqlx::postgres::PgPoolOptions::new().max_connections(connections).connect(&url).await.expect("connect")
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
