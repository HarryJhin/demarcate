mod db {
    use demarcate::diesel_async::postgres as driver;

    pub const KIND: super::Kind = super::Kind::Postgres;

    pub type Connection = diesel_async::AsyncPgConnection;

    pub fn placeholders(sql: &str) -> String {
        sql.to_owned()
    }

    include!("shared/diesel_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        pool(
            &std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set"),
            connections,
        )
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
