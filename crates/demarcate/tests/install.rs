// Every test installs its manager under a name of its own: the registry is one per process.

use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use demarcate::connection::{PoolSettings, SettingsError};
use demarcate::{InstallError, SetupError, TransactionError, transactional};
use sea_orm::ConnectionTrait;
use sqlx::SqlitePool;

const PASSWORD: &str = "hunter2-do-not-print";

#[derive(Debug)]
#[allow(dead_code)]
enum Error {
    Transaction(TransactionError),
    Sql(sqlx::Error),
    Orm(sea_orm::DbErr),
    Postgres(tokio_postgres::Error),
    Rejected,
}

impl From<TransactionError> for Error {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<sea_orm::DbErr> for Error {
    fn from(error: sea_orm::DbErr) -> Self {
        Self::Orm(error)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Postgres(error)
    }
}

fn sqlite_url(name: &str) -> String {
    let path = std::env::temp_dir().join(format!("demarcate-install-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    format!("sqlite:{}?mode=rwc", path.display())
}

async fn sqlite(name: &str) -> SqlitePool {
    let pool = SqlitePool::connect(&sqlite_url(name)).await.unwrap();
    sqlx::query("create table notes(v text)").execute(&pool).await.unwrap();
    pool
}

async fn notes(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar("select v from notes order by v").fetch_all(pool).await.unwrap()
}

async fn add(pool: &demarcate::sqlite::TransactionalPool, note: &str) -> Result<(), Error> {
    sqlx::query("insert into notes values($1)").bind(note.to_owned()).execute(pool).await?;
    Ok(())
}

struct Named {
    pool: demarcate::sqlite::TransactionalPool,
}

impl Named {
    #[transactional(transaction_manager = "install-named")]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        add(&self.pool, note).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

#[tokio::test]
async fn install_as_registers_a_manager_for_the_functions_that_name_it() {
    let raw = sqlite("named").await;
    let other = sqlite("named-other").await;

    let service = Named { pool: demarcate::sqlite::install_as("install-named", raw.clone()).unwrap() };
    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&raw).await, ["kept"]);

    assert_eq!(
        demarcate::sqlite::install_as("install-named", other).map(|_| ()),
        Err(InstallError::NameAlreadyInstalled("install-named".into()))
    );
    service.add("later", false).await.unwrap();
    assert_eq!(notes(&raw).await, ["kept", "later"]);
}

struct Wrapped {
    pool: demarcate::sqlite::TransactionalPool,
}

impl Wrapped {
    #[transactional(transaction_manager = "install-wrapped")]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        add(&self.pool, note).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

#[tokio::test]
async fn install_accepts_a_transactional_pool_and_keeps_its_clones_in_the_transaction() {
    let raw = sqlite("wrapped").await;
    let pool = demarcate::sqlite::TransactionalPool::new(raw.clone());
    let service = Wrapped { pool: pool.clone() };

    let installed = demarcate::sqlite::install_as("install-wrapped", pool).unwrap();

    assert_eq!(installed.resource_key(), service.pool.resource_key());
    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&raw).await, ["kept"]);
}

struct Configured {
    pool: demarcate::sqlite::TransactionalPool,
}

impl Configured {
    #[transactional(transaction_manager = "install-configured")]
    async fn add_and_try(&self, note: &str, attempt: &str) -> Result<(), Error> {
        add(&self.pool, note).await?;
        assert!(matches!(self.attempt(attempt).await, Err(Error::Rejected)));
        Ok(())
    }

    #[transactional(transaction_manager = "install-configured", propagation = Nested)]
    async fn attempt(&self, note: &str) -> Result<(), Error> {
        add(&self.pool, note).await?;
        Err(Error::Rejected)
    }

    #[transactional(transaction_manager = "install-configured")]
    async fn add_after(&self, wait: Duration, note: &str) -> Result<(), Error> {
        tokio::time::sleep(wait).await;
        add(&self.pool, note).await
    }
}

#[tokio::test]
async fn install_from_applies_the_settings_and_keeps_nested_transactions() {
    let settings = PoolSettings::new(sqlite_url("configured"))
        .max_connections(3)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(7))
        .idle_timeout(Duration::from_secs(90))
        .transaction_timeout(Duration::from_secs(1));

    let pool = demarcate::sqlite::install_from_as("install-configured", &settings).await.unwrap();
    let raw = pool.inner().clone();
    sqlx::query("create table notes(v text)").execute(&raw).await.unwrap();
    let service = Configured { pool };

    let options = raw.options();
    assert_eq!(options.get_max_connections(), 3);
    assert_eq!(options.get_min_connections(), 1);
    assert_eq!(options.get_acquire_timeout(), Duration::from_secs(7));
    assert_eq!(options.get_idle_timeout(), Some(Duration::from_secs(90)));

    service.add_and_try("outer", "inner").await.unwrap();
    assert_eq!(notes(&raw).await, ["outer"]);

    let late = service.add_after(Duration::from_millis(1200), "late").await;
    assert!(
        matches!(&late, Err(Error::Sql(sqlx::Error::Configuration(error)))
            if matches!(error.downcast_ref::<TransactionError>(), Some(TransactionError::TimedOut(_)))),
        "{late:?}"
    );
    assert_eq!(notes(&raw).await, ["outer"]);
}

struct Orm {
    database: demarcate::sea_orm::TransactionalDatabase,
}

impl Orm {
    #[transactional(transaction_manager = "install-orm")]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        self.database.execute_unprepared(&format!("insert into notes values('{note}')")).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

#[tokio::test]
async fn install_from_connects_a_sea_orm_connection() {
    let url = sqlite_url("orm");
    let settings = PoolSettings::new(url.clone()).max_connections(2);

    let database = demarcate::sea_orm::install_from_as("install-orm", &settings).await.unwrap();
    database.inner().execute_unprepared("create table notes(v text)").await.unwrap();
    let service = Orm { database };

    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&SqlitePool::connect(&url).await.unwrap()).await, ["kept"]);
}

struct Clients {
    pool: demarcate::tokio_postgres::TransactionalPool,
}

impl Clients {
    #[transactional(transaction_manager = "install-tokio-postgres")]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        self.pool.client().await?.execute("insert into install_tokio_postgres_notes values($1)", &[&note]).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

fn deadpool_postgres(url: &str) -> deadpool_postgres::Pool {
    let config = tokio_postgres::Config::from_str(url).unwrap();
    deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(config, tokio_postgres::NoTls)).build().unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL: set DEMARCATE_TEST_DATABASE_URL and pass --include-ignored"]
async fn install_as_registers_a_tokio_postgres_pool() {
    let url = std::env::var("DEMARCATE_TEST_DATABASE_URL").expect("DEMARCATE_TEST_DATABASE_URL must be set");
    let raw = deadpool_postgres(&url);
    let client = raw.get().await.unwrap();
    client
        .batch_execute(
            "drop table if exists install_tokio_postgres_notes; create table install_tokio_postgres_notes(v text)",
        )
        .await
        .unwrap();

    let service = Clients { pool: demarcate::tokio_postgres::install_as("install-tokio-postgres", raw).unwrap() };
    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));

    let rows = client.query("select v from install_tokio_postgres_notes", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>(), ["kept"]);
}

macro_rules! diesel_pool {
    ($connection:ty, $url:expr) => {
        diesel_async::pooled_connection::deadpool::Pool::builder(
            diesel_async::pooled_connection::AsyncDieselConnectionManager::<$connection>::new($url),
        )
        .build()
        .unwrap()
    };
}

#[tokio::test]
async fn install_as_registers_the_pools_that_connect_later() {
    let postgres = "postgres://app@127.0.0.1:1/app";
    let mysql = "mysql://app@127.0.0.1:1/app";

    assert!(demarcate::tokio_postgres::install_as("install-lazy-client", deadpool_postgres(postgres)).is_ok());
    assert!(
        demarcate::diesel_async::postgres::install_as(
            "install-lazy-diesel-postgres",
            diesel_pool!(diesel_async::AsyncPgConnection, postgres)
        )
        .is_ok()
    );
    assert!(
        demarcate::diesel_async::mysql::install_as(
            "install-lazy-diesel-mysql",
            diesel_pool!(diesel_async::AsyncMysqlConnection, mysql)
        )
        .is_ok()
    );

    assert_eq!(
        demarcate::tokio_postgres::install_as("install-lazy-diesel-mysql", deadpool_postgres(postgres)).map(|_| ()),
        Err(InstallError::NameAlreadyInstalled("install-lazy-diesel-mysql".into()))
    );
    assert_eq!(
        demarcate::diesel_async::postgres::install_as(
            "install-lazy-client",
            diesel_pool!(diesel_async::AsyncPgConnection, postgres)
        )
        .map(|_| ()),
        Err(InstallError::NameAlreadyInstalled("install-lazy-client".into()))
    );
}

fn assert_hides_the_password<E: std::fmt::Debug + std::fmt::Display>(driver: &str, error: SetupError<E>) {
    assert!(matches!(error, SetupError::Connect(_)), "{driver}: {error}");
    let text = format!("{error} {error:?}");
    assert!(!text.contains(PASSWORD), "{driver}: {text}");
}

#[tokio::test]
async fn a_failed_connection_does_not_print_the_password() {
    let quick = |url: String| PoolSettings::new(url).acquire_timeout(Duration::from_secs(2));
    let unreachable = |scheme: &str| quick(format!("{scheme}://app:{PASSWORD}@127.0.0.1:1/app"));
    let malformed = |scheme: &str| quick(format!("{scheme}://app:{PASSWORD}@[not-a-host/app"));

    for settings in [unreachable("postgres"), malformed("postgres")] {
        let error =
            demarcate::postgres::install_from_as("install-unreachable", &settings).await.map(|_| ()).unwrap_err();
        assert_hides_the_password("sqlx postgres", error);
    }
    for settings in [unreachable("mysql"), malformed("mysql")] {
        let error = demarcate::mysql::install_from_as("install-unreachable", &settings).await.map(|_| ()).unwrap_err();
        assert_hides_the_password("sqlx mysql", error);
    }
    for settings in [unreachable("postgres"), unreachable("mysql"), malformed("postgres")] {
        let error =
            demarcate::sea_orm::install_from_as("install-unreachable", &settings).await.map(|_| ()).unwrap_err();
        assert_hides_the_password("sea-orm", error);
    }

    let raw = sqlite("after-unreachable").await;
    assert!(demarcate::sqlite::install_as("install-unreachable", raw).is_ok());
}

const CHILD: &str = "DEMARCATE_INSTALL_TEST_CHILD";

struct Defaults {
    pool: demarcate::sqlite::TransactionalPool,
}

impl Defaults {
    #[transactional]
    async fn add(&self, note: &str, reject: bool) -> Result<(), Error> {
        add(&self.pool, note).await?;
        if reject { Err(Error::Rejected) } else { Ok(()) }
    }
}

async fn child_with_settings() {
    let pool = demarcate::sqlite::install_from_env().await.unwrap();
    let raw = pool.inner().clone();
    sqlx::query("create table notes(v text)").execute(&raw).await.unwrap();
    assert_eq!(raw.options().get_max_connections(), 4);

    let service = Defaults { pool };
    service.add("kept", false).await.unwrap();
    assert!(matches!(service.add("dropped", true).await, Err(Error::Rejected)));
    assert_eq!(notes(&raw).await, ["kept"]);

    let again = demarcate::sqlite::install_from_env().await.map(|_| ());
    assert!(matches!(again, Err(SetupError::Install(InstallError::DefaultAlreadyInstalled))), "{again:?}");
}

async fn child_without_url() {
    let missing = demarcate::sqlite::install_from_env().await.map(|_| ());
    assert!(
        matches!(&missing, Err(SetupError::Settings(SettingsError::Missing { variable })) if variable == "DATABASE_URL"),
        "{missing:?}"
    );
}

fn child(role: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", "install_from_env_reads_the_environment_of_the_process", "--include-ignored"]);
    command.env(CHILD, role);
    for variable in [
        "DATABASE_URL",
        "DATABASE_MAX_CONNECTIONS",
        "DATABASE_MIN_CONNECTIONS",
        "DATABASE_ACQUIRE_TIMEOUT_SECONDS",
        "DATABASE_IDLE_TIMEOUT_SECONDS",
        "DATABASE_TRANSACTION_TIMEOUT_SECONDS",
    ] {
        command.env_remove(variable);
    }
    command
}

// `install_from_env` reads the environment of the process and installs the default manager, so
// each case runs in a process of its own: this test starts itself again with a role.
#[tokio::test]
async fn install_from_env_reads_the_environment_of_the_process() {
    match std::env::var(CHILD).ok().as_deref() {
        Some("with-settings") => child_with_settings().await,
        Some("without-url") => child_without_url().await,
        Some(role) => panic!("unknown role {role}"),
        None => {
            let with_settings = child("with-settings")
                .env("DATABASE_URL", sqlite_url("from-env"))
                .env("DATABASE_MAX_CONNECTIONS", "4")
                .output()
                .unwrap();
            let without_url = child("without-url").output().unwrap();

            for (role, output) in [("with-settings", with_settings), ("without-url", without_url)] {
                let text = String::from_utf8_lossy(&output.stdout);
                assert!(output.status.success(), "{role}: {text}{}", String::from_utf8_lossy(&output.stderr));
                assert!(text.contains("1 passed"), "{role}: {text}");
            }
        }
    }
}
