mod db {
    pub const KIND: super::Kind = super::Kind::Postgres;

    pub fn placeholders(sql: &str) -> String {
        sql.to_owned()
    }

    include!("shared/sea_orm_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
        let mut options = sea_orm::ConnectOptions::new(url);
        options.max_connections(connections);
        sea_orm::Database::connect(options).await.expect("connect")
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
