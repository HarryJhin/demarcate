mod db {
    pub use demarcate::mysql::{TransactionalPool, manager};

    pub const KIND: super::Kind = super::Kind::MySql;

    pub fn placeholders(sql: &str) -> String {
        (1..=9).fold(sql.to_owned(), |sql, index| sql.replace(&format!("${index}"), "?"))
    }

    pub type Raw = sqlx::MySqlPool;

    include!("shared/sqlx_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        let url = std::env::var("DEMARCATE_TEST_MARIADB_URL").expect("DEMARCATE_TEST_MARIADB_URL must be set");
        sqlx::mysql::MySqlPoolOptions::new().max_connections(connections).connect(&url).await.expect("connect")
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
include!("shared/mysql_deadlock.rs");
