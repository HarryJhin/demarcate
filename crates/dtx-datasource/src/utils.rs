use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dtx_core::{
    BoundResource, BoxError, BoxFuture, CompletionStatus, Isolation, ResourceHolder, ResourceKey, TransactionDefinition,
    TransactionError, TransactionSynchronization, synchronization,
};

use crate::connection::{DataSource, SharedConnection, SqlError, SqlErrorKind, Statement};
use crate::holder::ConnectionHolder;

pub const CONNECTION_SYNCHRONIZATION_ORDER: i32 = 1000;

pub fn data_source_key(data_source: &Arc<dyn DataSource>) -> ResourceKey {
    ResourceKey::of(data_source)
}

pub(crate) fn bound_holder(data_source: &Arc<dyn DataSource>) -> Option<Arc<ConnectionHolder>> {
    synchronization::resource(&data_source_key(data_source)).and_then(|resource| resource.downcast::<ConnectionHolder>())
}

fn cannot_get_connection(source: impl Into<BoxError>) -> TransactionError {
    TransactionError::CannotGetConnection { message: "Failed to obtain JDBC Connection".into(), source: Some(source.into()) }
}

pub async fn connection(data_source: &Arc<dyn DataSource>) -> Result<SharedConnection, TransactionError> {
    let holder = bound_holder(data_source);
    if let Some(holder) = &holder
        && (holder.has_connection() || holder.is_synchronized_with_transaction())
    {
        holder.requested();
        if !holder.has_connection() {
            holder.set_connection(Some(data_source.connection().await.map_err(cannot_get_connection)?));
        }
        return holder.connection().map_err(cannot_get_connection);
    }

    let connection = data_source.connection().await.map_err(cannot_get_connection)?;

    if synchronization::is_synchronization_active() {
        let registered = (|| {
            let holder_to_use = match &holder {
                Some(holder) => {
                    holder.set_connection(Some(connection.clone()));
                    holder.clone()
                }
                None => Arc::new(ConnectionHolder::new(connection.clone())),
            };
            holder_to_use.requested();
            synchronization::register_synchronization(Arc::new(ConnectionSynchronization::new(
                holder_to_use.clone(),
                data_source.clone(),
            )))?;
            holder_to_use.set_synchronized_with_transaction(true);
            if holder.is_none() {
                synchronization::bind_resource(data_source_key(data_source), BoundResource::holder(holder_to_use))?;
            }
            Ok::<(), TransactionError>(())
        })();
        if let Err(error) = registered {
            release_connection(Some(&connection), Some(data_source)).await;
            return Err(cannot_get_connection(error));
        }
    }
    Ok(connection)
}

pub async fn prepare_connection_for_transaction(
    connection: &SharedConnection,
    definition: Option<&TransactionDefinition>,
) -> Result<Option<Isolation>, SqlError> {
    prepare_connection(
        connection,
        definition.map_or(Isolation::Default, |definition| definition.isolation),
        definition.is_some_and(|definition| definition.read_only),
    )
    .await
}

pub(crate) async fn prepare_connection(
    connection: &SharedConnection,
    isolation: Isolation,
    set_read_only: bool,
) -> Result<Option<Isolation>, SqlError> {
    let mut connection = connection.lock().await;
    if set_read_only && let Err(error) = connection.set_read_only(true).await {
        if error.kind == SqlErrorKind::Timeout {
            return Err(error);
        }
        tracing::debug!(%error, "Could not set JDBC Connection read-only");
    }
    let mut previous_isolation = None;
    if isolation != Isolation::Default {
        let current_isolation = connection.transaction_isolation().await?;
        if current_isolation != isolation {
            previous_isolation = Some(current_isolation);
            connection.set_transaction_isolation(isolation).await?;
        }
    }
    Ok(previous_isolation)
}

pub async fn reset_connection_after_transaction(
    connection: &SharedConnection,
    previous_isolation: Option<Isolation>,
    reset_read_only: bool,
) {
    let mut connection = connection.lock().await;
    let result: Result<(), SqlError> = async {
        if let Some(previous_isolation) = previous_isolation {
            connection.set_transaction_isolation(previous_isolation).await?;
        }
        if reset_read_only {
            connection.set_read_only(false).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        tracing::debug!(%error, "Could not reset JDBC Connection after transaction");
    }
}

pub fn is_connection_transactional(connection: &SharedConnection, data_source: Option<&Arc<dyn DataSource>>) -> bool {
    data_source
        .and_then(bound_holder)
        .is_some_and(|holder| connection_equals(&holder, connection))
}

pub async fn apply_transaction_timeout(
    statement: &mut dyn Statement,
    data_source: Option<&Arc<dyn DataSource>>,
) -> Result<(), TransactionError> {
    apply_timeout(statement, data_source, -1).await
}

pub async fn apply_timeout(
    statement: &mut dyn Statement,
    data_source: Option<&Arc<dyn DataSource>>,
    timeout: i32,
) -> Result<(), TransactionError> {
    let holder = data_source.and_then(bound_holder);
    let seconds = match holder {
        Some(holder) if holder.has_timeout() => Some(holder.time_to_live_in_seconds()?),
        _ if timeout >= 0 => Some(timeout),
        _ => None,
    };
    if let Some(seconds) = seconds {
        statement
            .set_query_timeout(seconds)
            .await
            .map_err(|error| TransactionError::Other(error.into()))?;
    }
    Ok(())
}

pub async fn release_connection(connection: Option<&SharedConnection>, data_source: Option<&Arc<dyn DataSource>>) {
    if let Err(error) = do_release_connection(connection, data_source).await {
        tracing::debug!(%error, "Could not close JDBC Connection");
    }
}

pub async fn do_release_connection(
    connection: Option<&SharedConnection>,
    data_source: Option<&Arc<dyn DataSource>>,
) -> Result<(), SqlError> {
    let Some(connection) = connection else {
        return Ok(());
    };
    if let Some(data_source) = data_source
        && let Some(holder) = bound_holder(data_source)
        && connection_equals(&holder, connection)
    {
        holder.released();
        return Ok(());
    }
    do_close_connection(connection, data_source).await
}

pub async fn do_close_connection(
    connection: &SharedConnection,
    data_source: Option<&Arc<dyn DataSource>>,
) -> Result<(), SqlError> {
    if data_source.is_none_or(|data_source| data_source.should_close(connection)) {
        connection.lock().await.close().await?;
    }
    Ok(())
}

fn connection_equals(holder: &ConnectionHolder, passed_in: &SharedConnection) -> bool {
    if !holder.has_connection() {
        return false;
    }
    holder.connection().is_ok_and(|held| Arc::ptr_eq(&held, passed_in))
}

struct ConnectionSynchronization {
    connection_holder: Arc<ConnectionHolder>,
    data_source: Arc<dyn DataSource>,
    order: i32,
    holder_active: AtomicBool,
}

impl ConnectionSynchronization {
    fn new(connection_holder: Arc<ConnectionHolder>, data_source: Arc<dyn DataSource>) -> Self {
        Self { connection_holder, data_source, order: CONNECTION_SYNCHRONIZATION_ORDER, holder_active: AtomicBool::new(true) }
    }
    fn key(&self) -> ResourceKey {
        data_source_key(&self.data_source)
    }
}

impl TransactionSynchronization for ConnectionSynchronization {
    fn order(&self) -> i32 {
        self.order
    }

    fn suspend(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            if self.holder_active.load(Ordering::SeqCst) {
                synchronization::unbind_resource(&self.key())?;
                if self.connection_holder.has_connection() && !self.connection_holder.is_open() {
                    let connection = self.connection_holder.connection()?;
                    release_connection(Some(&connection), Some(&self.data_source)).await;
                    self.connection_holder.set_connection(None);
                }
            }
            Ok(())
        })
    }

    fn resume(&self) -> BoxFuture<'_, Result<(), TransactionError>> {
        Box::pin(async {
            if self.holder_active.load(Ordering::SeqCst) {
                synchronization::bind_resource(self.key(), BoundResource::holder(self.connection_holder.clone()))?;
            }
            Ok(())
        })
    }

    fn before_completion(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async {
            if !self.connection_holder.is_open() {
                synchronization::unbind_resource(&self.key())?;
                self.holder_active.store(false, Ordering::SeqCst);
                if self.connection_holder.has_connection() {
                    let connection = self.connection_holder.connection()?;
                    release_connection(Some(&connection), Some(&self.data_source)).await;
                }
            }
            Ok(())
        })
    }

    fn after_completion(&self, _status: CompletionStatus) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async {
            if self.holder_active.load(Ordering::SeqCst) {
                synchronization::unbind_resource_if_possible(&self.key());
                self.holder_active.store(false, Ordering::SeqCst);
                if self.connection_holder.has_connection() {
                    let connection = self.connection_holder.connection()?;
                    release_connection(Some(&connection), Some(&self.data_source)).await;
                    self.connection_holder.set_connection(None);
                }
            }
            self.connection_holder.reset();
            Ok(())
        })
    }
}
