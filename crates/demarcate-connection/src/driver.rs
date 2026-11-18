use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use demarcate_core::{
    BoundResource, ManagerSettings, Savepoint, TransactionDefinition, TransactionDriver, TransactionError,
    TransactionManager, synchronization,
};

use crate::{ConnectionBackend, ConnectionHolder, ConnectionSource};

#[doc(hidden)]
pub struct ConnectionTransactionObject<B: ConnectionBackend> {
    holder: Mutex<Option<Arc<ConnectionHolder<B>>>>,
    new_connection_holder: AtomicBool,
    savepoint_allowed: bool,
}

impl<B: ConnectionBackend> ConnectionTransactionObject<B> {
    fn holder(&self) -> Option<Arc<ConnectionHolder<B>>> {
        self.holder.lock().expect("holder lock").clone()
    }
    fn set_holder(&self, holder: Option<Arc<ConnectionHolder<B>>>, new_connection_holder: bool) {
        *self.holder.lock().expect("holder lock") = holder;
        self.new_connection_holder.store(new_connection_holder, Ordering::SeqCst);
    }
    fn is_new_connection_holder(&self) -> bool {
        self.new_connection_holder.load(Ordering::SeqCst)
    }
    fn holder_for_savepoint(&self) -> Result<Arc<ConnectionHolder<B>>, TransactionError> {
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

/// The [`TransactionDriver`] for a [`ConnectionBackend`].
///
/// A nested transaction is a savepoint on the connection of the outer transaction. While it
/// runs, that connection belongs to the nested call, and a future beside it that uses the
/// connection gets an error.
pub struct ConnectionDriver<B: ConnectionBackend> {
    source: ConnectionSource<B>,
}

impl<B: ConnectionBackend> ConnectionDriver<B> {
    /// Creates a driver that runs transactions on the connections of the given source.
    pub fn new(source: ConnectionSource<B>) -> Self {
        Self { source }
    }
}

enum SavepointError<E> {
    State(TransactionError),
    Backend(E),
}

impl<E: std::error::Error + Send + Sync + 'static> SavepointError<E> {
    fn into_system(self, message: &str) -> TransactionError {
        match self {
            Self::State(error) => error,
            Self::Backend(error) => TransactionError::system(message, error),
        }
    }
}

fn savepoint_name(savepoint: &Savepoint) -> Result<&str, TransactionError> {
    savepoint
        .downcast_ref::<String>()
        .map(String::as_str)
        .ok_or_else(|| TransactionError::Usage("Savepoint was not created by this transaction manager".into()))
}

impl<B: ConnectionBackend> ConnectionDriver<B> {
    async fn execute_on(
        &self,
        holder: &ConnectionHolder<B>,
        statement: String,
    ) -> Result<(), SavepointError<B::Error>> {
        synchronization::check_access().map_err(SavepointError::State)?;
        holder.check_savepoint_owner().map_err(SavepointError::State)?;
        let mut guard = holder.lock().map_err(SavepointError::State)?;
        let connection = guard.as_mut().ok_or_else(|| {
            SavepointError::State(TransactionError::Usage(
                "The transaction of this status has no open connection. It has completed or its call was \
                 cancelled, so a savepoint can no longer be used on it"
                    .into(),
            ))
        })?;
        match self.source.backend().execute(connection, &statement).await {
            Ok(()) => Ok(()),
            Err(error) => {
                holder.mark_aborted();
                Err(SavepointError::Backend(error))
            }
        }
    }
}

impl<B: ConnectionBackend> TransactionDriver for ConnectionDriver<B> {
    type Transaction = ConnectionTransactionObject<B>;
    type Suspended = Arc<ConnectionHolder<B>>;

    fn transaction(&self, settings: &ManagerSettings) -> Result<Self::Transaction, TransactionError> {
        Ok(ConnectionTransactionObject {
            holder: Mutex::new(self.source.bound_holder()),
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
        self.source.backend().validate(definition)?;
        let started = self.source.backend().begin(definition).await.map_err(|error| {
            TransactionError::CannotCreateTransaction {
                message: "Could not open a database connection for the transaction".into(),
                source: Some(error.into()),
            }
        })?;
        let holder = Arc::new(ConnectionHolder::new(started));
        holder.set_synchronized_with_transaction(true);
        holder.set_transaction_active(true);
        if let Some(timeout) = definition.timeout.or(settings.default_timeout) {
            holder.set_timeout(timeout);
        }
        transaction.set_holder(Some(holder.clone()), true);
        if let Err(error) =
            synchronization::bind_resource(self.source.resource_key().clone(), BoundResource::holder(holder.clone()))
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
        synchronization::unbind_resource(self.source.resource_key())?
            .downcast::<ConnectionHolder<B>>()
            .ok_or_else(|| TransactionError::InvalidState("Bound resource is not a connection holder".into()))
    }

    async fn resume(
        &self,
        _transaction: Option<&Self::Transaction>,
        suspended: Self::Suspended,
    ) -> Result<(), TransactionError> {
        synchronization::bind_resource(self.source.resource_key().clone(), BoundResource::holder(suspended))
    }

    async fn commit(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let holder =
            transaction.holder().ok_or_else(|| TransactionError::Usage("No connection holder for commit".into()))?;
        let connection = holder.lock()?.take();
        match connection {
            Some(mut started) if B::in_transaction(&started) => {
                let backend = self.source.backend();
                if holder.is_aborted() && !backend.is_usable(&mut started).await {
                    backend
                        .rollback(started)
                        .await
                        .map_err(|error| TransactionError::system("Rollback failed", error))?;
                    return Err(TransactionError::UnexpectedRollback(B::UNUSABLE_MESSAGE.into()));
                }
                backend.commit(started).await.map_err(|error| TransactionError::system("Commit failed", error))
            }
            Some(_) | None => Ok(()),
        }
    }

    async fn rollback(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        let holder =
            transaction.holder().ok_or_else(|| TransactionError::Usage("No connection holder for rollback".into()))?;
        let connection = holder.lock()?.take();
        match connection {
            Some(started) if B::in_transaction(&started) => self
                .source
                .backend()
                .rollback(started)
                .await
                .map_err(|error| TransactionError::system("Rollback failed", error)),
            Some(_) | None => Ok(()),
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
            synchronization::unbind_resource_if_possible(self.source.resource_key());
        }
        if let Some(holder) = transaction.holder() {
            if new_holder {
                holder.release_connection().await;
            }
            holder.clear();
        }
    }

    fn abandon(&self, transaction: &Self::Transaction) {
        if transaction.is_new_connection_holder()
            && let Some(holder) = transaction.holder()
        {
            holder.abandon();
        }
    }

    fn own_savepoint(&self, transaction: &Self::Transaction, savepoint: &Savepoint) {
        if let (Some(holder), Ok(name)) = (transaction.holder(), savepoint_name(savepoint)) {
            holder.savepoint_owned(name);
        }
    }

    fn set_rollback_only_on_abandon(&self, transaction: &Self::Transaction) -> Result<(), TransactionError> {
        if let Some(holder) = transaction.holder() {
            holder.set_rollback_only_for_good();
        }
        Ok(())
    }

    fn abandon_savepoint(&self, transaction: &Self::Transaction, savepoint: &Savepoint) {
        if let (Some(holder), Ok(name)) = (transaction.holder(), savepoint_name(savepoint)) {
            holder.savepoint_gone(name);
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
        self.execute_on(&holder, format!("SAVEPOINT {name}")).await.map_err(|error| match error {
            SavepointError::State(error) => error,
            SavepointError::Backend(error) => TransactionError::CannotCreateTransaction {
                message: "Could not create a savepoint".into(),
                source: Some(error.into()),
            },
        })?;
        holder.savepoint_created(&name);
        Ok(Arc::new(name))
    }

    async fn rollback_to_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.holder_for_savepoint()?;
        let name = savepoint_name(savepoint)?;
        self.execute_on(&holder, format!("ROLLBACK TO SAVEPOINT {name}"))
            .await
            .map_err(|error| error.into_system("Could not roll back to the savepoint"))?;
        holder.savepoint_rolled_back(name);
        Ok(())
    }

    async fn release_savepoint(
        &self,
        transaction: &Self::Transaction,
        savepoint: &Savepoint,
    ) -> Result<(), TransactionError> {
        let holder = transaction.holder_for_savepoint()?;
        let name = savepoint_name(savepoint)?;
        let released = self.execute_on(&holder, format!("RELEASE SAVEPOINT {name}")).await;
        holder.savepoint_gone(name);
        match released {
            Ok(()) => Ok(()),
            Err(SavepointError::Backend(error)) if B::release_failure_is_ignorable(&error) => Ok(()),
            Err(error) => Err(error.into_system("Could not release the savepoint")),
        }
    }
}

/// Creates a transaction manager for the given source, with nested transactions allowed.
pub fn manager<B: ConnectionBackend>(source: ConnectionSource<B>) -> TransactionManager<ConnectionDriver<B>> {
    manager_with_settings(source, ManagerSettings { nested_transaction_allowed: true, ..ManagerSettings::default() })
}

/// Creates a transaction manager for the given source with the given settings.
pub fn manager_with_settings<B: ConnectionBackend>(
    source: ConnectionSource<B>,
    settings: ManagerSettings,
) -> TransactionManager<ConnectionDriver<B>> {
    TransactionManager::with_settings(ConnectionDriver::new(source), settings)
}
