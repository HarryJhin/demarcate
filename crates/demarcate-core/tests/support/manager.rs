use std::sync::{Arc, Mutex};

use demarcate_core::{
    BoundResource, BoxError, Isolation, ManagerSettings, Savepoint, TransactionDefinition, TransactionDriver,
    TransactionError, TransactionManager, synchronization,
};

use super::connection::{DataSource, SharedConnection, SqlError, SqlErrorKind};
use super::holder::{ConnectionHolder, SqlOrState};
use super::utils;

pub use super::utils::data_source_key;

#[derive(Default)]
struct ObjectState {
    connection_holder: Option<Arc<ConnectionHolder>>,
    previous_isolation: Option<Isolation>,
    read_only: bool,
    savepoint_allowed: bool,
    new_connection_holder: bool,
    must_restore_auto_commit: bool,
}

pub struct DataSourceTransactionObject {
    state: Mutex<ObjectState>,
}

impl DataSourceTransactionObject {
    fn state(&self) -> std::sync::MutexGuard<'_, ObjectState> {
        self.state.lock().expect("transaction object lock")
    }

    pub fn set_connection_holder(&self, connection_holder: Option<Arc<ConnectionHolder>>, new_connection_holder: bool) {
        let mut state = self.state();
        state.connection_holder = connection_holder;
        state.new_connection_holder = new_connection_holder;
    }

    fn clear_connection_holder(&self) {
        self.state().connection_holder = None;
    }

    pub fn connection_holder(&self) -> Result<Arc<ConnectionHolder>, TransactionError> {
        self.state()
            .connection_holder
            .clone()
            .ok_or_else(|| TransactionError::InvalidState("No ConnectionHolder available".into()))
    }

    pub fn has_connection_holder(&self) -> bool {
        self.state().connection_holder.is_some()
    }

    pub fn is_new_connection_holder(&self) -> bool {
        self.state().new_connection_holder
    }

    pub fn previous_isolation(&self) -> Option<Isolation> {
        self.state().previous_isolation
    }

    pub fn is_read_only(&self) -> bool {
        self.state().read_only
    }

    pub fn is_savepoint_allowed(&self) -> bool {
        self.state().savepoint_allowed
    }

    pub fn is_must_restore_auto_commit(&self) -> bool {
        self.state().must_restore_auto_commit
    }

    fn connection_holder_for_savepoint(&self) -> Result<Arc<ConnectionHolder>, TransactionError> {
        if !self.is_savepoint_allowed() {
            return Err(TransactionError::NestedNotSupported(
                "Transaction manager does not allow nested transactions".into(),
            ));
        }
        if !self.has_connection_holder() {
            return Err(TransactionError::Usage(
                "Cannot create nested transaction when not exposing a JDBC transaction".into(),
            ));
        }
        self.connection_holder()
    }
}

pub struct DataSourceTransactionDriver {
    data_source: Arc<dyn DataSource>,
    enforce_read_only: bool,
    default_read_only: Mutex<Option<bool>>,
}

impl DataSourceTransactionDriver {
    pub fn new(data_source: Arc<dyn DataSource>) -> Self {
        Self { data_source, enforce_read_only: false, default_read_only: Mutex::new(None) }
    }

    pub fn enforce_read_only(mut self, enforce_read_only: bool) -> Self {
        self.enforce_read_only = enforce_read_only;
        self
    }

    pub fn is_enforce_read_only(&self) -> bool {
        self.enforce_read_only
    }

    pub fn data_source(&self) -> &Arc<dyn DataSource> {
        &self.data_source
    }

    async fn check_default_read_only(&self, connection: &SharedConnection) {
        if self.default_read_only.lock().expect("default read-only lock").is_some() {
            return;
        }
        let read_only = connection.lock().await.is_read_only().await.unwrap_or(false);
        *self.default_read_only.lock().expect("default read-only lock") = Some(read_only);
    }

    fn is_default_read_only(&self) -> bool {
        *self.default_read_only.lock().expect("default read-only lock") == Some(true)
    }

    async fn prepare_transactional_connection(
        &self,
        connection: &SharedConnection,
        definition: &TransactionDefinition,
    ) -> Result<(), SqlError> {
        if self.enforce_read_only && definition.read_only {
            let mut statement = connection.lock().await.create_statement().await?;
            let executed = statement.execute_update("SET TRANSACTION READ ONLY").await;
            statement.close().await?;
            executed?;
        }
        Ok(())
    }

    fn translate_error(&self, task: &str, error: SqlError) -> TransactionError {
        TransactionError::system(format!("{task} failed"), error)
    }

    async fn begin_on(
        &self,
        transaction: &DataSourceTransactionObject,
        definition: &TransactionDefinition,
        settings: &ManagerSettings,
        connection_slot: &mut Option<SharedConnection>,
    ) -> Result<(), BoxError> {
        let needs_connection = match transaction.state().connection_holder.clone() {
            None => true,
            Some(holder) => holder.is_synchronized_with_transaction(),
        };
        if needs_connection {
            let new_connection = self.data_source.connection().await?;
            if definition.read_only {
                self.check_default_read_only(&new_connection).await;
            }
            transaction.set_connection_holder(Some(Arc::new(ConnectionHolder::new(new_connection))), true);
        }

        let holder = transaction.connection_holder()?;
        holder.set_synchronized_with_transaction(true);
        let connection = holder.connection()?;
        *connection_slot = Some(connection.clone());

        let previous_isolation = utils::prepare_connection(
            &connection,
            definition.isolation,
            definition.read_only && !self.is_default_read_only(),
        )
        .await?;
        {
            let mut state = transaction.state();
            state.previous_isolation = previous_isolation;
            state.read_only = definition.read_only;
        }

        {
            let mut guard = connection.lock().await;
            if guard.auto_commit().await? {
                transaction.state().must_restore_auto_commit = true;
                guard.set_auto_commit(false).await?;
            }
        }

        self.prepare_transactional_connection(&connection, definition).await?;
        holder.set_transaction_active(true);

        if let Some(timeout) = definition.timeout.or(settings.default_timeout) {
            holder.set_timeout(timeout);
        }

        if transaction.is_new_connection_holder() {
            synchronization::bind_resource(data_source_key(&self.data_source), BoundResource::holder(holder))?;
        }
        Ok(())
    }
}

impl TransactionDriver for DataSourceTransactionDriver {
    type Transaction = DataSourceTransactionObject;
    type Suspended = BoundResource;

    fn transaction(&self, settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError> {
        Ok(DataSourceTransactionObject {
            state: Mutex::new(ObjectState {
                savepoint_allowed: settings.nested_transaction_allowed,
                connection_holder: utils::bound_holder(&self.data_source),
                new_connection_holder: false,
                ..ObjectState::default()
            }),
        })
    }

    fn is_existing_transaction(&self, transaction: &Self::Transaction) -> bool {
        transaction.state().connection_holder.as_ref().is_some_and(|holder| holder.is_transaction_active())
    }

    async fn begin(
        &self,
        transaction: &Self::Transaction,
        definition: &TransactionDefinition,
        settings: &ManagerSettings,
    ) -> Result<(), TransactionError> {
        let mut connection = None;
        if let Err(error) = self.begin_on(transaction, definition, settings, &mut connection).await {
            if transaction.is_new_connection_holder() {
                utils::release_connection(connection.as_ref(), Some(&self.data_source)).await;
                transaction.set_connection_holder(None, false);
            }
            return Err(TransactionError::CannotCreateTransaction {
                message: "Could not open JDBC Connection for transaction".into(),
                source: Some(error),
            });
        }
        Ok(())
    }

    async fn suspend(&self, transaction: &Self::Transaction) -> Result<Self::Suspended, TransactionError> {
        transaction.clear_connection_holder();
        synchronization::unbind_resource(&data_source_key(&self.data_source))
    }

    async fn resume(
        &self,
        _transaction: Option<&Self::Transaction>,
        suspended: Self::Suspended,
    ) -> Result<(), TransactionError> {
        synchronization::bind_resource(data_source_key(&self.data_source), suspended)
    }

    async fn commit(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let connection = transaction.connection_holder()?.connection()?;
        let result = connection.lock().await.commit().await;
        result.map_err(|error| self.translate_error("JDBC commit", error))
    }

    async fn rollback(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let connection = transaction.connection_holder()?.connection()?;
        let result = connection.lock().await.rollback().await;
        result.map_err(|error| self.translate_error("JDBC rollback", error))
    }

    fn set_rollback_only(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        transaction.connection_holder()?.set_rollback_only();
        Ok(())
    }

    async fn cleanup_after_completion(&self, transaction: &Self::Transaction) {
        if transaction.is_new_connection_holder() {
            synchronization::unbind_resource_if_possible(&data_source_key(&self.data_source));
        }
        let Ok(holder) = transaction.connection_holder() else {
            return;
        };
        let Ok(connection) = holder.connection() else {
            holder.clear();
            return;
        };
        if transaction.is_must_restore_auto_commit()
            && let Err(error) = connection.lock().await.set_auto_commit(true).await
        {
            tracing::debug!(%error, "Could not reset JDBC Connection after transaction");
        } else {
            utils::reset_connection_after_transaction(
                &connection,
                transaction.previous_isolation(),
                transaction.is_read_only() && !self.is_default_read_only(),
            )
            .await;
        }
        if transaction.is_new_connection_holder() {
            utils::release_connection(Some(&connection), Some(&self.data_source)).await;
        }
        holder.clear();
    }

    fn abandon(&self, transaction: &Self::Transaction) {
        if transaction.is_new_connection_holder() {
            synchronization::unbind_resource_if_possible(&data_source_key(&self.data_source));
        }
        if let Ok(holder) = transaction.connection_holder() {
            holder.clear();
        }
    }

    fn resume_abandoned(&self, suspended: Self::Suspended) {
        let _ = synchronization::bind_resource(data_source_key(&self.data_source), suspended);
    }

    fn is_rollback_only(&self, transaction: &Self::Transaction) -> bool {
        transaction.state().connection_holder.as_ref().is_some_and(|holder| holder.is_rollback_only())
    }

    async fn flush(&self, _transaction: &Self::Transaction) -> Result<(), TransactionError> {
        synchronization::trigger_flush().await
    }

    async fn create_savepoint(&self, transaction: &Self::Transaction) -> Result<Savepoint, TransactionError> {
        let holder = transaction.connection_holder_for_savepoint()?;
        let cannot_create = |error: SqlOrState| match error {
            SqlOrState::Sql(error) => TransactionError::CannotCreateTransaction {
                message: "Could not create JDBC savepoint".into(),
                source: Some(error.into()),
            },
            SqlOrState::State(error) => error,
        };
        if !holder.supports_savepoints().await.map_err(cannot_create)? {
            return Err(TransactionError::NestedNotSupported(
                "Cannot create a nested transaction because savepoints are not supported by your JDBC driver".into(),
            ));
        }
        if holder.is_rollback_only() {
            return Err(TransactionError::CannotCreateTransaction {
                message: "Cannot create savepoint for transaction which is already marked as rollback-only".into(),
                source: None,
            });
        }
        holder.create_savepoint().await.map_err(cannot_create)
    }

    async fn rollback_to_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.connection_holder_for_savepoint()?;
        let connection = holder
            .connection()
            .map_err(|error| TransactionError::system("Could not roll back to JDBC savepoint", error))?;
        let result = connection.lock().await.rollback_to_savepoint(savepoint).await;
        result.map_err(|error| TransactionError::system("Could not roll back to JDBC savepoint", error))?;
        holder.reset_rollback_only();
        Ok(())
    }

    async fn release_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.connection_holder_for_savepoint()?;
        let connection = holder
            .connection()
            .map_err(|error| TransactionError::system("Could not explicitly release JDBC savepoint", error))?;
        let result = connection.lock().await.release_savepoint(savepoint).await;
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.kind == SqlErrorKind::FeatureNotSupported => Ok(()),
            Err(error) if error.sql_state.as_deref() == Some("3B001") => Ok(()),
            Err(error) if error.message.contains("not supported") || error.message.contains("3B001") => Ok(()),
            Err(error) => Err(TransactionError::system("Could not explicitly release JDBC savepoint", error)),
        }
    }
}

pub type DataSourceTransactionManager = TransactionManager<DataSourceTransactionDriver>;

impl DataSourceTransactionDriver {
    pub fn into_manager(self) -> DataSourceTransactionManager {
        self.into_manager_with(ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() })
    }

    pub fn into_manager_with(self, settings: ManagerSettings) -> DataSourceTransactionManager {
        TransactionManager::with_settings(self, settings)
    }
}
