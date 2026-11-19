use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use demarcate_core::{PlatformTransactionManager, TransactionError, TransactionStatus};

use crate::attribute::TransactionAttribute;

#[derive(Default)]
struct Managers {
    default: Option<Arc<dyn PlatformTransactionManager>>,
    named: HashMap<String, Arc<dyn PlatformTransactionManager>>,
}

/// Published when a `#[transactional]` function rolls back because it returned an error or
/// panicked.
///
/// Register a listener with
/// [`TransactionManagerRegistry::add_rollback_listener`]. The event is published before the
/// rollback happens.
pub struct FunctionRollbackEvent {
    /// The path of the function, or its descriptor when one is set.
    pub function: String,
    /// The type name of the error, or `"panic"`.
    pub failure: &'static str,
    /// The status of the transaction that is about to roll back.
    pub transaction: Arc<dyn TransactionStatus>,
}

type RollbackListener = Arc<dyn Fn(&FunctionRollbackEvent) + Send + Sync>;

/// The transaction managers that `#[transactional]` functions use.
///
/// An application registers its managers once at startup: one default manager, and named
/// managers for functions that say `transaction_manager = "name"`. The global registry is
/// [`registry`].
#[derive(Default)]
pub struct TransactionManagerRegistry {
    managers: RwLock<Managers>,
    rollback_listeners: RwLock<Vec<RollbackListener>>,
}

impl TransactionManagerRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the manager for functions that do not name one.
    pub fn set_default(&self, manager: Arc<dyn PlatformTransactionManager>) {
        self.managers.write().expect("registry lock").default = Some(manager);
    }

    /// Registers a manager under a name.
    pub fn register(&self, qualifier: impl Into<String>, manager: Arc<dyn PlatformTransactionManager>) {
        self.managers.write().expect("registry lock").named.insert(qualifier.into(), manager);
    }

    /// Sets the manager for functions that do not name one, unless one is set.
    ///
    /// Returns whether the manager was set.
    pub fn set_default_if_absent(&self, manager: Arc<dyn PlatformTransactionManager>) -> bool {
        let mut managers = self.managers.write().expect("registry lock");
        if managers.default.is_some() {
            return false;
        }
        managers.default = Some(manager);
        true
    }

    /// Registers a manager under a name, unless one is registered under that name.
    ///
    /// Returns whether the manager was registered.
    pub fn register_if_absent(
        &self,
        qualifier: impl Into<String>,
        manager: Arc<dyn PlatformTransactionManager>,
    ) -> bool {
        let mut managers = self.managers.write().expect("registry lock");
        match managers.named.entry(qualifier.into()) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(manager);
                true
            }
        }
    }

    /// Registers a listener for [`FunctionRollbackEvent`].
    pub fn add_rollback_listener(&self, listener: impl Fn(&FunctionRollbackEvent) + Send + Sync + 'static) {
        self.rollback_listeners.write().expect("registry lock").push(Arc::new(listener));
    }

    pub(crate) fn publish_rollback(&self, event: &FunctionRollbackEvent) {
        let listeners = self.rollback_listeners.read().expect("registry lock").clone();
        for listener in listeners {
            listener(event);
        }
    }

    /// Returns the manager for the given attribute: the one registered under its qualifier, or the
    /// default manager.
    ///
    /// # Errors
    ///
    /// Returns an error that names the missing registration when no such manager is registered.
    pub fn determine_transaction_manager(
        &self,
        attribute: &TransactionAttribute,
    ) -> Result<Arc<dyn PlatformTransactionManager>, TransactionError> {
        let managers = self.managers.read().expect("registry lock");
        if let Some(qualifier) = attribute.qualifier.as_deref().filter(|qualifier| !qualifier.trim().is_empty()) {
            return managers.named.get(qualifier).cloned().ok_or_else(|| {
                TransactionError::InvalidState(format!(
                    "No transaction manager is registered with qualifier '{qualifier}'. Install it with \
                     install_as(\"{qualifier}\", pool) of the module of your connection library, such as \
                     demarcate::postgres, before the first \
                     #[transactional(transaction_manager = \"{qualifier}\")] call"
                ))
            });
        }
        managers.default.clone().ok_or_else(|| {
            TransactionError::InvalidState(
                "No default transaction manager is registered. Call install(pool) of the module of your \
                 connection library, such as demarcate::postgres::install, at application startup, or \
                 register a manager with demarcate::registry().set_default(manager), or name one \
                 with #[transactional(transaction_manager = \"...\")]"
                    .into(),
            )
        })
    }
}

/// Returns the global registry that `#[transactional]` functions use.
pub fn registry() -> &'static TransactionManagerRegistry {
    static REGISTRY: OnceLock<TransactionManagerRegistry> = OnceLock::new();
    REGISTRY.get_or_init(TransactionManagerRegistry::new)
}
