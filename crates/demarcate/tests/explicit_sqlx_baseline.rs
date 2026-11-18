use sqlx::{PgConnection, PgPool, Row};

#[derive(Debug)]
enum ServiceError {
    Sql(#[allow(dead_code)] sqlx::Error),
    Rejected,
}

impl From<sqlx::Error> for ServiceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

struct Repository;

impl Repository {
    async fn insert(&self, connection: &mut PgConnection, value: &str) -> Result<(), ServiceError> {
        sqlx::query("insert into demarcate_baseline values($1)").bind(value.to_owned()).execute(connection).await?;
        Ok(())
    }
}

struct Service {
    pool: PgPool,
    repository: Repository,
}

impl Service {
    async fn register(&self, value: &str) -> Result<(), ServiceError> {
        let mut tx = self.pool.begin().await?;
        self.register_in(&mut tx, value).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn register_in(&self, tx: &mut PgConnection, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(tx, value).await
    }

    async fn register_then_fail_in(&self, tx: &mut PgConnection, value: &str) -> Result<(), ServiceError> {
        self.repository.insert(tx, value).await?;
        Err(ServiceError::Rejected)
    }

    async fn register_two(&self, first: &str, second: &str) -> Result<(), ServiceError> {
        let mut tx = self.pool.begin().await?;
        self.register_in(&mut tx, first).await?;
        self.register_then_fail_in(&mut tx, second).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn audit(&self, value: &str) -> Result<(), ServiceError> {
        let mut tx = self.pool.begin().await?;
        self.repository.insert(&mut tx, value).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn register_with_audit_then_fail(&self, value: &str) -> Result<(), ServiceError> {
        let mut tx = self.pool.begin().await?;
        self.repository.insert(&mut tx, value).await?;
        self.audit("audit").await?;
        Err(ServiceError::Rejected)
    }
}

async fn rows(pool: &PgPool) -> Vec<String> {
    sqlx::query("select v from demarcate_baseline order by v")
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn explicit_transactions_on_postgres() {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
    sqlx::query("drop table if exists demarcate_baseline").execute(&pool).await.unwrap();
    sqlx::query("create table demarcate_baseline(v text)").execute(&pool).await.unwrap();
    let service = Service { pool: pool.clone(), repository: Repository };

    service.register("a").await.unwrap();
    assert_eq!(rows(&pool).await, ["a"]);

    sqlx::query("truncate demarcate_baseline").execute(&pool).await.unwrap();
    assert!(matches!(service.register_two("a", "b").await, Err(ServiceError::Rejected)));
    assert!(rows(&pool).await.is_empty());

    sqlx::query("truncate demarcate_baseline").execute(&pool).await.unwrap();
    assert!(matches!(service.register_with_audit_then_fail("a").await, Err(ServiceError::Rejected)));
    assert_eq!(rows(&pool).await, ["audit"]);
}
