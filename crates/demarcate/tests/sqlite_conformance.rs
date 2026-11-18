mod db {
    use std::time::Duration;

    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

    pub use demarcate::sqlite::{TransactionalPool, manager};

    pub const KIND: super::Kind = super::Kind::Sqlite;

    pub fn placeholders(sql: &str) -> String {
        sql.to_owned()
    }

    pub type Raw = sqlx::SqlitePool;

    include!("shared/sqlx_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        let path = std::env::temp_dir().join(format!("demarcate-conformance-{}-{connections}.db", std::process::id()));
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(500));
        SqlitePoolOptions::new().max_connections(connections).connect_with(options).await.expect("connect")
    }
}

macro_rules! db_test {
    ($name:ident, |$service:ident| $body:block) => {
        #[test]
        fn $name() {
            run_test(|$service| Box::pin(async move $body));
        }
    };
}

include!("shared/conformance.rs");
