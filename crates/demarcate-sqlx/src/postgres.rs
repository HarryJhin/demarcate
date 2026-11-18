use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use demarcate_core::{
    BoundResource, BoxError, BoxFuture, CompletionStatus, Isolation, ManagerSettings, ResourceHolder, ResourceKey,
    Savepoint, TransactionDefinition, TransactionDriver, TransactionError, TransactionManager,
    TransactionSynchronization, synchronization,
};
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres, Transaction};
use tokio::sync::OwnedMutexGuard;

pub const CONNECTION_SYNCHRONIZATION_ORDER: i32 = 1000;

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[doc(hidden)]
pub enum Conn {
    Pooled(PoolConnection<Postgres>),
    Transaction(Transaction<'static, Postgres>),
}

impl Conn {
    fn connection(&mut self) -> &mut PgConnection {
        match self {
            Self::Pooled(connection) => connection,
            Self::Transaction(transaction) => transaction,
        }
    }
    fn connection_ref(&self) -> &PgConnection {
        match self {
            Self::Pooled(connection) => connection,
            Self::Transaction(transaction) => transaction,
        }
    }
}

pub struct ConnectionHolder {
    connection: Arc<tokio::sync::Mutex<Option<Conn>>>,
    synchronized_with_transaction: AtomicBool,
    rollback_only: AtomicBool,
    transaction_active: AtomicBool,
    deadline: Mutex<Option<Instant>>,
    savepoint_counter: AtomicU32,
    reference_count: AtomicUsize,
}

impl ConnectionHolder {
    fn new(connection: Conn) -> Self {
        Self {
            connection: Arc::new(tokio::sync::Mutex::new(Some(connection))),
            synchronized_with_transaction: AtomicBool::new(false),
            rollback_only: AtomicBool::new(false),
            transaction_active: AtomicBool::new(false),
            deadline: Mutex::new(None),
            savepoint_counter: AtomicU32::new(0),
            reference_count: AtomicUsize::new(0),
        }
    }
    pub fn is_synchronized_with_transaction(&self) -> bool {
        self.synchronized_with_transaction.load(Ordering::SeqCst)
    }
    pub fn set_synchronized_with_transaction(&self, value: bool) {
        self.synchronized_with_transaction.store(value, Ordering::SeqCst);
    }
    pub fn is_transaction_active(&self) -> bool {
        self.transaction_active.load(Ordering::SeqCst)
    }
    fn set_transaction_active(&self, value: bool) {
        self.transaction_active.store(value, Ordering::SeqCst);
    }
    pub fn set_rollback_only(&self) {
        self.rollback_only.store(true, Ordering::SeqCst);
    }
    pub fn reset_rollback_only(&self) {
        self.rollback_only.store(false, Ordering::SeqCst);
    }
    pub fn is_rollback_only(&self) -> bool {
        self.rollback_only.load(Ordering::SeqCst)
    }
    pub fn set_timeout(&self, timeout: Duration) {
        *self.deadline.lock().expect("deadline lock") = Some(Instant::now() + timeout);
    }
    pub fn has_timeout(&self) -> bool {
        self.deadline.lock().expect("deadline lock").is_some()
    }
    pub fn time_to_live(&self) -> Result<Option<Duration>, TransactionError> {
        let Some(deadline) = *self.deadline.lock().expect("deadline lock") else {
            return Ok(None);
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.set_rollback_only();
            return Err(TransactionError::TimedOut("Transaction timed out: deadline has passed".into()));
        }
        Ok(Some(remaining))
    }
    pub fn requested(&self) {
        self.reference_count.fetch_add(1, Ordering::SeqCst);
    }
    pub fn released(&self) {
        let mut current = self.reference_count.load(Ordering::SeqCst);
        while current > 0 {
            match self.reference_count.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
    pub fn is_open(&self) -> bool {
        self.reference_count.load(Ordering::SeqCst) > 0
    }
    fn next_savepoint_name(&self) -> String {
        format!("SAVEPOINT_{}", self.savepoint_counter.fetch_add(1, Ordering::SeqCst) + 1)
    }
    fn clear(&self) {
        self.set_synchronized_with_transaction(false);
        self.reset_rollback_only();
        *self.deadline.lock().expect("deadline lock") = None;
        self.set_transaction_active(false);
        self.savepoint_counter.store(0, Ordering::SeqCst);
    }
    async fn release_connection(&self) {
        match self.connection.try_lock() {
            Ok(mut connection) => {
                connection.take();
            }
            Err(_) => tracing::error!(
                "A TransactionalConnection outlived its transaction; the connection is released when that handle is dropped"
            ),
        }
    }
    fn lock(&self) -> Result<OwnedMutexGuard<Option<Conn>>, TransactionError> {
        self.connection.clone().try_lock_owned().map_err(|_| {
            TransactionError::InvalidState(
                "The connection of the current transaction is already in use on this task. A \
                 TransactionalConnection returned by acquire() is still alive, or two queries of one transaction \
                 are awaited concurrently (join!, select!). Drop the connection before calling another \
                 transactional function, and await the queries of one transaction one after another"
                    .into(),
            )
        })
    }

    fn abandon(&self) {
        self.set_transaction_active(false);
        self.clear();
        if let Ok(mut connection) = self.connection.try_lock() {
            connection.take();
        }
    }
}

impl ResourceHolder for ConnectionHolder {
    fn reset(&self) {
        self.clear();
        self.reference_count.store(0, Ordering::SeqCst);
    }
    fn unbound(&self) {}
    fn is_void(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub struct TransactionalPool {
    inner: PgPool,
    key: ResourceKey,
    transaction_required: bool,
}

impl TransactionalPool {
    pub fn new(pool: PgPool) -> Self {
        let id = NEXT_POOL_ID.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: pool,
            key: ResourceKey::named(format!("demarcate-sqlx-postgres-{id}")),
            transaction_required: false,
        }
    }

    pub fn transaction_required(&self) -> Self {
        Self { transaction_required: true, ..self.clone() }
    }

    pub fn is_transaction_required(&self) -> bool {
        self.transaction_required
    }

    pub fn inner(&self) -> &PgPool {
        &self.inner
    }

    pub fn resource_key(&self) -> &ResourceKey {
        &self.key
    }

    fn bound_holder(&self) -> Option<Arc<ConnectionHolder>> {
        synchronization::resource(&self.key).and_then(|resource| resource.downcast::<ConnectionHolder>())
    }

    pub fn is_connection_transactional(&self) -> bool {
        self.bound_holder().is_some()
    }

    pub async fn acquire_in_transaction(&self) -> Result<TransactionalConnection, TransactionError> {
        if !self.bound_holder().is_some_and(|holder| holder.is_transaction_active()) {
            return Err(TransactionError::NoTransaction(
                "No transaction is active for this pool on the current task. acquire_in_transaction() must be \
                 called inside a #[transactional] function or TransactionTemplate::execute that uses this \
                 pool's transaction manager. A task created with tokio::spawn does not inherit the transaction; \
                 open a new one inside the spawned task, or use acquire() to run without a transaction"
                    .into(),
            ));
        }
        self.acquire_any().await
    }

    pub async fn acquire(&self) -> Result<TransactionalConnection, TransactionError> {
        if self.transaction_required {
            return self.acquire_in_transaction().await;
        }
        self.acquire_any().await
    }

    async fn acquire_any(&self) -> Result<TransactionalConnection, TransactionError> {
        let synchronized = synchronization::is_synchronization_active();
        if let Some(holder) = self.bound_holder() {
            let mut guard = holder.lock()?;
            if guard.is_none() {
                *guard = Some(if holder.is_transaction_active() {
                    tracing::warn!(
                        "Data access after the transaction completed: it participates in the completed \
                         transaction and is not committed. Run it in a RequiresNew transaction to keep it"
                    );
                    Conn::Transaction(self.inner.begin().await.map_err(cannot_get_connection)?)
                } else {
                    Conn::Pooled(self.inner.acquire().await.map_err(cannot_get_connection)?)
                });
            }
            apply_timeout(&holder, guard.as_mut().expect("connection present")).await?;
            holder.requested();
            return Ok(TransactionalConnection(Handle::Held { guard, holder }));
        }

        let connection = self.inner.acquire().await.map_err(cannot_get_connection)?;
        if !synchronized {
            return Ok(TransactionalConnection(Handle::Standalone(connection)));
        }

        let holder = Arc::new(ConnectionHolder::new(Conn::Pooled(connection)));
        synchronization::register_synchronization(Arc::new(ConnectionSynchronization {
            holder: holder.clone(),
            key: self.key.clone(),
            holder_active: AtomicBool::new(true),
        }))?;
        holder.set_synchronized_with_transaction(true);
        synchronization::bind_resource(self.key.clone(), BoundResource::holder(holder.clone()))?;
        let guard = holder.lock()?;
        holder.requested();
        Ok(TransactionalConnection(Handle::Held { guard, holder }))
    }
}

fn into_sqlx(error: TransactionError) -> sqlx::Error {
    sqlx::Error::Configuration(Box::new(error))
}

impl<'p> sqlx::Executor<'p> for &'_ TransactionalPool {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<sqlx::Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>, sqlx::Error>,
    >
    where
        'p: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        use futures_util::TryStreamExt;
        let pool = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            let mut rows = (&mut *connection).fetch_many(query);
            while let Some(step) = rows.try_next().await? {
                yield step;
            }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::future::BoxFuture<'e, Result<Option<sqlx::postgres::PgRow>, sqlx::Error>>
    where
        'p: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            (&mut *connection).fetch_optional(query).await
        })
    }

    fn prepare_with<'e>(
        self,
        sql: sqlx::SqlStr,
        parameters: &'e [sqlx::postgres::PgTypeInfo],
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::postgres::PgStatement, sqlx::Error>>
    where
        'p: 'e,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            (&mut *connection).prepare_with(sql, parameters).await
        })
    }

    fn describe<'e>(
        self,
        sql: sqlx::SqlStr,
    ) -> futures_core::future::BoxFuture<'e, Result<sqlx::Describe<Postgres>, sqlx::Error>>
    where
        'p: 'e,
    {
        let pool = self.clone();
        Box::pin(async move {
            let mut connection = pool.acquire().await.map_err(into_sqlx)?;
            (&mut *connection).describe(sql).await
        })
    }
}

fn cannot_get_connection(error: sqlx::Error) -> TransactionError {
    TransactionError::CannotGetConnection { message: "Failed to obtain connection".into(), source: Some(error.into()) }
}

async fn apply_timeout(holder: &ConnectionHolder, connection: &mut Conn) -> Result<(), TransactionError> {
    let Some(remaining) = holder.time_to_live()? else {
        return Ok(());
    };
    if let Conn::Transaction(transaction) = connection {
        let millis = remaining.as_millis().max(1).to_string();
        sqlx::query("select set_config('statement_timeout', $1, true)")
            .bind(millis)
            .execute(&mut **transaction)
            .await
            .map_err(|error| TransactionError::system("Could not apply transaction timeout", error))?;
    }
    Ok(())
}

enum Handle {
    Held { guard: OwnedMutexGuard<Option<Conn>>, holder: Arc<ConnectionHolder> },
    Standalone(PoolConnection<Postgres>),
}

pub struct TransactionalConnection(Handle);

impl Deref for TransactionalConnection {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Handle::Held { guard, .. } => guard.as_ref().expect("connection present").connection_ref(),
            Handle::Standalone(connection) => connection,
        }
    }
}

impl DerefMut for TransactionalConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            Handle::Held { guard, .. } => guard.as_mut().expect("connection present").connection(),
            Handle::Standalone(connection) => connection,
        }
    }
}

impl Drop for TransactionalConnection {
    fn drop(&mut self) {
        if let Handle::Held { holder, .. } = &self.0 {
            holder.released();
        }
    }
}

struct ConnectionSynchronization {
    holder: Arc<ConnectionHolder>,
    key: ResourceKey,
    holder_active: AtomicBool,
}

impl TransactionSynchronization for ConnectionSynchronization {
    fn order(&self) -> i32 {
        CONNECTION_SYNCHRONIZATION_ORDER
    }
    fn suspend(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            if self.holder_active.load(Ordering::SeqCst) {
                synchronization::unbind_resource_if_possible(&self.key);
                if !self.holder.is_open() {
                    self.holder.release_connection().await;
                }
            }
            Ok(())
        })
    }
    fn resume(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            if self.holder_active.load(Ordering::SeqCst) {
                synchronization::bind_resource(self.key.clone(), BoundResource::holder(self.holder.clone()))?;
            }
            Ok(())
        })
    }
    fn before_completion(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async {
            if !self.holder.is_open() {
                synchronization::unbind_resource_if_possible(&self.key);
                self.holder_active.store(false, Ordering::SeqCst);
                self.holder.release_connection().await;
            }
            Ok(())
        })
    }
    fn after_completion(&self, _status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async {
            if self.holder_active.swap(false, Ordering::SeqCst) {
                synchronization::unbind_resource_if_possible(&self.key);
                self.holder.release_connection().await;
            }
            self.holder.reset();
            Ok(())
        })
    }
}

#[doc(hidden)]
pub struct PgTransactionObject {
    holder: Mutex<Option<Arc<ConnectionHolder>>>,
    new_connection_holder: AtomicBool,
    savepoint_allowed: bool,
}

impl PgTransactionObject {
    fn holder(&self) -> Option<Arc<ConnectionHolder>> {
        self.holder.lock().expect("holder lock").clone()
    }
    fn set_holder(&self, holder: Option<Arc<ConnectionHolder>>, new_connection_holder: bool) {
        *self.holder.lock().expect("holder lock") = holder;
        self.new_connection_holder.store(new_connection_holder, Ordering::SeqCst);
    }
    fn is_new_connection_holder(&self) -> bool {
        self.new_connection_holder.load(Ordering::SeqCst)
    }
    fn holder_for_savepoint(&self) -> Result<Arc<ConnectionHolder>, TransactionError> {
        if !self.savepoint_allowed {
            return Err(TransactionError::NestedNotSupported(
                "Transaction manager does not allow nested transactions".into(),
            ));
        }
        self.holder().ok_or_else(|| {
            TransactionError::Usage("Cannot create a nested transaction when no database transaction is exposed".into())
        })
    }
}

pub struct PgTransactionDriver {
    pool: TransactionalPool,
}

fn begin_statement(definition: &TransactionDefinition) -> String {
    let mut statement = String::from("BEGIN");
    match definition.isolation {
        Isolation::Default => {}
        Isolation::ReadUncommitted => statement.push_str(" ISOLATION LEVEL READ UNCOMMITTED"),
        Isolation::ReadCommitted => statement.push_str(" ISOLATION LEVEL READ COMMITTED"),
        Isolation::RepeatableRead => statement.push_str(" ISOLATION LEVEL REPEATABLE READ"),
        Isolation::Serializable => statement.push_str(" ISOLATION LEVEL SERIALIZABLE"),
    }
    if definition.read_only {
        statement.push_str(" READ ONLY");
    }
    statement
}

async fn execute_on(holder: &ConnectionHolder, statement: String) -> Result<(), SavepointError> {
    let mut guard = holder.lock().map_err(SavepointError::State)?;
    let connection = guard
        .as_mut()
        .ok_or_else(|| SavepointError::State(TransactionError::Usage("Connection holder has no connection".into())))?;
    sqlx::query(AssertSqlSafe(statement))
        .execute(connection.connection())
        .await
        .map(|_| ())
        .map_err(SavepointError::Sql)
}

enum SavepointError {
    State(TransactionError),
    Sql(sqlx::Error),
}

impl SavepointError {
    fn into_system(self, message: &str) -> TransactionError {
        match self {
            Self::State(error) => error,
            Self::Sql(error) => TransactionError::system(message, error),
        }
    }
}

fn savepoint_name(savepoint: &Savepoint) -> Result<&str, TransactionError> {
    savepoint
        .downcast_ref::<String>()
        .map(String::as_str)
        .ok_or_else(|| TransactionError::Usage("Savepoint was not created by this transaction manager".into()))
}

impl TransactionDriver for PgTransactionDriver {
    type Transaction = PgTransactionObject;
    type Suspended = Arc<ConnectionHolder>;

    fn transaction(&self, settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError> {
        Ok(PgTransactionObject {
            holder: Mutex::new(self.pool.bound_holder()),
            new_connection_holder: AtomicBool::new(false),
            savepoint_allowed: settings.nested_transaction_allowed,
        })
    }

    fn is_existing_transaction(&self, transaction: &Self::Transaction) -> bool {
        transaction.holder().is_some_and(|holder| holder.is_transaction_active())
    }

    async fn begin(
        &self,
        transaction: &Self::Transaction,
        definition: &TransactionDefinition,
        settings: &ManagerSettings,
    ) -> Result<(), TransactionError> {
        let existing = transaction.holder();
        if let Some(holder) = &existing
            && !holder.is_synchronized_with_transaction()
        {
            return Err(TransactionError::CannotCreateTransaction {
                message: "Could not open connection for transaction: a connection holder without \
                              transaction synchronization is already bound"
                    .into(),
                source: None,
            });
        }
        let started =
            self.pool.inner.begin_with(AssertSqlSafe(begin_statement(definition))).await.map_err(|error| {
                TransactionError::CannotCreateTransaction {
                    message: "Could not open a database connection for the transaction".into(),
                    source: Some(error.into()),
                }
            })?;
        let holder = Arc::new(ConnectionHolder::new(Conn::Transaction(started)));
        holder.set_synchronized_with_transaction(true);
        holder.set_transaction_active(true);
        if let Some(timeout) = definition.timeout.or(settings.default_timeout) {
            holder.set_timeout(timeout);
        }
        transaction.set_holder(Some(holder.clone()), true);
        if let Err(error) = synchronization::bind_resource(self.pool.key.clone(), BoundResource::holder(holder.clone()))
        {
            holder.release_connection().await;
            transaction.set_holder(None, false);
            return Err(TransactionError::CannotCreateTransaction {
                message: "Could not open a database connection for the transaction".into(),
                source: Some(error.into()),
            });
        }
        Ok(())
    }

    async fn suspend(&self, transaction: &Self::Transaction) -> Result<Self::Suspended, TransactionError> {
        transaction.set_holder(None, transaction.is_new_connection_holder());
        synchronization::unbind_resource(&self.pool.key)?
            .downcast::<ConnectionHolder>()
            .ok_or_else(|| TransactionError::InvalidState("Bound resource is not a connection holder".into()))
    }

    async fn resume(
        &self,
        _transaction: Option<&Self::Transaction>,
        suspended: Self::Suspended,
    ) -> Result<(), TransactionError> {
        synchronization::bind_resource(self.pool.key.clone(), BoundResource::holder(suspended))
    }

    async fn commit(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let holder =
            transaction.holder().ok_or_else(|| TransactionError::Usage("No connection holder for commit".into()))?;
        let connection = holder.lock()?.take();
        match connection {
            Some(Conn::Transaction(started)) => {
                started.commit().await.map_err(|error| TransactionError::system("Commit failed", error))
            }
            Some(Conn::Pooled(_)) | None => Ok(()),
        }
    }

    async fn rollback(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let holder =
            transaction.holder().ok_or_else(|| TransactionError::Usage("No connection holder for rollback".into()))?;
        let connection = holder.lock()?.take();
        match connection {
            Some(Conn::Transaction(started)) => {
                started.rollback().await.map_err(|error| TransactionError::system("Rollback failed", error))
            }
            Some(Conn::Pooled(_)) | None => Ok(()),
        }
    }

    fn set_rollback_only(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        if let Some(holder) = transaction.holder() {
            holder.set_rollback_only();
        }
        Ok(())
    }

    async fn cleanup_after_completion(&self, transaction: &Self::Transaction) {
        let new_holder = transaction.is_new_connection_holder();
        if new_holder {
            synchronization::unbind_resource_if_possible(&self.pool.key);
        }
        if let Some(holder) = transaction.holder() {
            if new_holder {
                holder.release_connection().await;
            }
            holder.clear();
        }
    }

    fn abandon(&self, transaction: &Self::Transaction) {
        if let Some(holder) = transaction.holder() {
            holder.abandon();
        }
    }

    fn is_rollback_only(&self, transaction: &Self::Transaction) -> bool {
        transaction.holder().is_some_and(|holder| holder.is_rollback_only())
    }

    async fn flush(&self, _transaction: &Self::Transaction) -> Result<(), TransactionError> {
        synchronization::trigger_flush().await
    }

    async fn create_savepoint(&self, transaction: &Self::Transaction) -> Result<Savepoint, TransactionError> {
        let holder = transaction.holder_for_savepoint()?;
        if holder.is_rollback_only() {
            return Err(TransactionError::CannotCreateTransaction {
                message: "Cannot create savepoint for transaction which is already marked as rollback-only".into(),
                source: None,
            });
        }
        let name = holder.next_savepoint_name();
        execute_on(&holder, format!("SAVEPOINT {name}")).await.map_err(|error| match error {
            SavepointError::State(error) => error,
            SavepointError::Sql(error) => TransactionError::CannotCreateTransaction {
                message: "Could not create a savepoint".into(),
                source: Some(error.into()),
            },
        })?;
        Ok(Arc::new(name))
    }

    async fn rollback_to_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.holder_for_savepoint()?;
        let name = savepoint_name(savepoint)?;
        execute_on(&holder, format!("ROLLBACK TO SAVEPOINT {name}"))
            .await
            .map_err(|error| error.into_system("Could not roll back to the savepoint"))?;
        holder.reset_rollback_only();
        Ok(())
    }

    async fn release_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.holder_for_savepoint()?;
        let name = savepoint_name(savepoint)?;
        match execute_on(&holder, format!("RELEASE SAVEPOINT {name}")).await {
            Ok(()) => Ok(()),
            Err(SavepointError::Sql(error)) if release_failure_is_ignorable(&error) => Ok(()),
            Err(error) => Err(error.into_system("Could not release the savepoint")),
        }
    }
}

fn release_failure_is_ignorable(error: &sqlx::Error) -> bool {
    let code_matches =
        error.as_database_error().and_then(|database| database.code()).is_some_and(|code| code == "3B001");
    let message = error.to_string();
    code_matches || message.contains("not supported") || message.contains("3B001")
}

pub type PgTransactionManager = TransactionManager<PgTransactionDriver>;

pub fn manager(pool: TransactionalPool) -> PgTransactionManager {
    manager_with_settings(pool, ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() })
}

pub fn manager_with_settings(pool: TransactionalPool, settings: ManagerSettings) -> PgTransactionManager {
    TransactionManager::with_settings(PgTransactionDriver { pool }, settings)
}
