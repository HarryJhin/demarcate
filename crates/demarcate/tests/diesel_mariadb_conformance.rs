mod db {
    use demarcate::diesel_async::mysql as driver;

    pub const KIND: super::Kind = super::Kind::MySql;

    pub type Connection = diesel_async::AsyncMysqlConnection;

    pub fn placeholders(sql: &str) -> String {
        (1..=9).fold(sql.to_owned(), |sql, index| sql.replace(&format!("${index}"), "?"))
    }

    include!("shared/diesel_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        pool(&std::env::var("DEMARCATE_TEST_MARIADB_URL").expect("DEMARCATE_TEST_MARIADB_URL must be set"), connections)
    }
}

macro_rules! db_test {
    ($name:ident, |$service:ident| $body:block) => {
        #[test]
        #[ignore = "requires MariaDB: set DEMARCATE_TEST_MARIADB_URL and pass --include-ignored"]
        fn $name() {
            run_test(|$service| Box::pin(async move $body));
        }
    };
}

include!("shared/conformance.rs");
