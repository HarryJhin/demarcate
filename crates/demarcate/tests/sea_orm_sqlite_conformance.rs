mod db {
    pub const KIND: super::Kind = super::Kind::Sqlite;

    pub fn placeholders(sql: &str) -> String {
        (1..=9).fold(sql.to_owned(), |sql, index| sql.replace(&format!("${index}"), "?"))
    }

    include!("shared/sea_orm_db.rs");

    pub async fn connect(connections: u32) -> Raw {
        use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
        static CLEAN: std::sync::Once = std::sync::Once::new();
        let path = std::env::temp_dir().join(format!("demarcate-sea-orm-{}.db", std::process::id()));
        CLEAN.call_once(|| {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
        });
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_millis(500));
        let pool = SqlitePoolOptions::new().max_connections(connections).connect_with(options).await.expect("connect");
        sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool)
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

mod note {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "demarcate_conformance")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub v: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl Service {
    #[transactional]
    async fn save_entities(&self, values: &[&str], fail: bool) -> Result<Vec<String>, ServiceError> {
        use sea_orm::{ActiveModelTrait, EntityTrait, QueryOrder, Set};
        for value in values {
            note::ActiveModel { v: Set((*value).to_owned()) }.insert(&self.pool).await?;
        }
        let found = note::Entity::find().order_by_asc(note::Column::V).all(&self.pool).await?;
        if fail {
            return Err(ServiceError::Rejected);
        }
        Ok(found.into_iter().map(|model| model.v).collect())
    }
}

db_test!(entity_operations_run_in_the_transaction, |service| {
    assert_eq!(service.save_entities(&["a", "b"], false).await.unwrap(), ["a", "b"]);
    assert_eq!(service.committed().await, ["a", "b"]);

    assert!(matches!(service.save_entities(&["c"], true).await, Err(ServiceError::Rejected)));
    assert_eq!(service.committed().await, ["a", "b"], "the entity inserted by the failed call is rolled back");
});
