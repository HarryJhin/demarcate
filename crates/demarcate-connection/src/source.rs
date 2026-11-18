use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use demarcate_core::{
    BoundResource, BoxError, BoxFuture, CompletionStatus, ResourceHolder, ResourceKey, TransactionDefinition,
    TransactionError, TransactionSynchronization, synchronization,
};

use crate::holder::Lease;
use crate::{ConnectionBackend, ConnectionHolder};

/// The position of the synchronization that releases a connection held for a scope without a
/// transaction. Synchronizations with a lower order run before the connection is released.
pub const CONNECTION_SYNCHRONIZATION_ORDER: i32 = 1000;

static NEXT_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// Hands out the connection of the current transaction.
///
/// Inside a transaction [`acquire`](ConnectionSource::acquire) returns the connection of that
/// transaction. Outside of one it returns a connection in autocommit mode, unless the source was
/// made with [`transaction_required`](ConnectionSource::transaction_required).
///
/// A crate for a connection library wraps this type in its own, which offers the query API of
/// the library.
///
/// Cloning is cheap; clones share the backend and the transaction.
pub struct ConnectionSource<B: ConnectionBackend> {
    backend: Arc<B>,
    key: ResourceKey,
    transaction_required: bool,
}

impl<B: ConnectionBackend> Clone for ConnectionSource<B> {
    fn clone(&self) -> Self {
        Self { backend: self.backend.clone(), key: self.key.clone(), transaction_required: self.transaction_required }
    }
}

impl<B: ConnectionBackend> fmt::Debug for ConnectionSource<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSource")
            .field("key", &self.key)
            .field("transaction_required", &self.transaction_required)
            .finish()
    }
}

impl<B: ConnectionBackend> ConnectionSource<B> {
    /// Creates a source for the given backend.
    ///
    /// Create one source for each pool and clone it. Two sources created for the same pool do not
    /// see each other's transactions.
    pub fn new(backend: B) -> Self {
        let id = NEXT_SOURCE_ID.fetch_add(1, Ordering::SeqCst);
        Self {
            backend: Arc::new(backend),
            key: ResourceKey::named(format!("{}-{id}", B::NAME)),
            transaction_required: false,
        }
    }

    /// Returns a source that refuses to hand out a connection outside a transaction.
    pub fn transaction_required(&self) -> Self {
        Self { transaction_required: true, ..self.clone() }
    }

    /// Returns whether this source refuses to hand out a connection outside a transaction.
    pub fn is_transaction_required(&self) -> bool {
        self.transaction_required
    }

    /// Returns the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub(crate) fn shared_backend(&self) -> Arc<B> {
        self.backend.clone()
    }

    /// Returns the key under which the connection of a transaction is bound to the context.
    pub fn resource_key(&self) -> &ResourceKey {
        &self.key
    }

    pub(crate) fn bound_holder(&self) -> Option<Arc<ConnectionHolder<B>>> {
        synchronization::resource(&self.key).and_then(|resource| resource.downcast::<ConnectionHolder<B>>())
    }

    /// Returns whether a connection of this source is bound to the context of the calling future.
    pub fn is_connection_transactional(&self) -> bool {
        self.bound_holder().is_some()
    }

    /// Returns the connection of the current transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::NoTransaction`] when the calling future runs in no transaction of
    /// this source, and otherwise the errors of [`acquire`](ConnectionSource::acquire).
    pub async fn acquire_in_transaction(&self) -> Result<ConnectionHandle<B>, TransactionError> {
        synchronization::check_access()?;
        if !self.bound_holder().is_some_and(|holder| holder.is_transaction_active()) {
            return Err(TransactionError::NoTransaction(
                "No transaction is active for this pool in the calling future. acquire_in_transaction() must be \
                 called inside a #[transactional] function or TransactionTemplate::execute that uses this \
                 pool's transaction manager. A task created with tokio::spawn does not inherit the transaction; \
                 open a new one inside the spawned task, or use acquire() to run without a transaction"
                    .into(),
            ));
        }
        self.acquire_any().await
    }

    /// Returns the connection of the current transaction, or a connection in autocommit mode when
    /// there is none.
    ///
    /// Drop the returned handle before the next use of the source and before calling another
    /// transactional function; while it is alive the connection is in use.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection of the transaction is already in use, when a nested
    /// transaction of another call owns it, when the transaction that the calling future joined has
    /// completed, or when no connection can be obtained. Returns [`TransactionError::TimedOut`] when
    /// the deadline of the transaction has passed, and [`TransactionError::NoTransaction`] when the
    /// source requires a transaction and none is running.
    pub async fn acquire(&self) -> Result<ConnectionHandle<B>, TransactionError> {
        if self.transaction_required {
            return self.acquire_in_transaction().await;
        }
        self.acquire_any().await
    }

    async fn acquire_any(&self) -> Result<ConnectionHandle<B>, TransactionError> {
        synchronization::check_access()?;
        let synchronized = synchronization::is_synchronization_active();
        if let Some(holder) = self.bound_holder() {
            holder.check_savepoint_owner()?;
            holder.check_not_ended()?;
            let mut guard = match holder.lock() {
                Ok(guard) => guard,
                Err(_) if !holder.is_transaction_active() => {
                    let connection = self.backend.acquire().await.map_err(cannot_get_connection)?;
                    return Ok(ConnectionHandle(Handle::Standalone(connection)));
                }
                Err(error) => return Err(error),
            };
            if guard.is_none() {
                *guard = Some(if holder.is_transaction_active() {
                    tracing::warn!(
                        "Data access after the transaction completed: it participates in the completed \
                         transaction and is not committed. Run it in a RequiresNew transaction to keep it"
                    );
                    self.backend.begin(&TransactionDefinition::new()).await.map_err(cannot_get_connection)?
                } else {
                    self.backend.acquire().await.map_err(cannot_get_connection)?
                });
            }
            if let Some(remaining) = holder.time_to_live()? {
                let connection = guard.as_mut().expect("connection present");
                if B::in_transaction(connection) {
                    self.backend
                        .apply_timeout(connection, remaining)
                        .await
                        .map_err(|error| TransactionError::system("Could not apply transaction timeout", error))?;
                }
            }
            holder.requested();
            return Ok(ConnectionHandle(Handle::Held { guard, holder }));
        }

        let connection = self.backend.acquire().await.map_err(cannot_get_connection)?;
        if !synchronized || self.bound_holder().is_some() {
            return Ok(ConnectionHandle(Handle::Standalone(connection)));
        }

        let holder = Arc::new(ConnectionHolder::new(self.backend.clone(), connection));
        synchronization::register_synchronization(Arc::new(ConnectionSynchronization {
            holder: holder.clone(),
            key: self.key.clone(),
            holder_active: AtomicBool::new(true),
        }))?;
        holder.set_synchronized_with_transaction(true);
        synchronization::bind_resource(self.key.clone(), BoundResource::holder(holder.clone()))?;
        let guard = holder.lock()?;
        holder.requested();
        Ok(ConnectionHandle(Handle::Held { guard, holder }))
    }
}

fn cannot_get_connection<E: Into<BoxError>>(error: E) -> TransactionError {
    TransactionError::CannotGetConnection { message: "Failed to obtain connection".into(), source: Some(error.into()) }
}

enum Handle<B: ConnectionBackend> {
    Held { guard: Lease<B::Connection>, holder: Arc<ConnectionHolder<B>> },
    Standalone(B::Connection),
}

/// A connection obtained from a [`ConnectionSource`]: the connection of the current transaction,
/// or a connection in autocommit mode.
///
/// It dereferences to the connection of the backend. Dropping it does not commit or roll back;
/// the transaction manager completes the transaction.
pub struct ConnectionHandle<B: ConnectionBackend>(Handle<B>);

impl<B: ConnectionBackend> ConnectionHandle<B> {
    /// Returns the holder of the transaction that this connection belongs to, or `None` when the
    /// connection runs no transaction of the transaction manager.
    pub fn transaction_holder(&self) -> Option<Arc<ConnectionHolder<B>>> {
        match &self.0 {
            Handle::Held { holder, .. } if holder.is_transaction_active() => Some(holder.clone()),
            _ => None,
        }
    }

    /// Records the outcome of a statement that ran on this connection, so that a commit can detect
    /// a transaction that a failed statement left unable to commit.
    pub fn statement_finished(&self, error: Option<&B::Error>) {
        if let (Some(holder), Some(error)) = (self.transaction_holder(), error) {
            holder.statement_failed(error);
        }
    }
}

impl<B: ConnectionBackend> Deref for ConnectionHandle<B> {
    type Target = B::Connection;
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Handle::Held { guard, .. } => guard.as_ref().expect("connection present"),
            Handle::Standalone(connection) => connection,
        }
    }
}

impl<B: ConnectionBackend> DerefMut for ConnectionHandle<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            Handle::Held { guard, .. } => guard.as_mut().expect("connection present"),
            Handle::Standalone(connection) => connection,
        }
    }
}

impl<B: ConnectionBackend> Drop for ConnectionHandle<B> {
    fn drop(&mut self) {
        if let Handle::Held { holder, .. } = &self.0 {
            holder.released();
        }
    }
}

struct ConnectionSynchronization<B: ConnectionBackend> {
    holder: Arc<ConnectionHolder<B>>,
    key: ResourceKey,
    holder_active: AtomicBool,
}

impl<B: ConnectionBackend> TransactionSynchronization for ConnectionSynchronization<B> {
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
