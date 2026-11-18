use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use demarcate_core::{PlatformTransactionManager, TransactionError};

use crate::attribute::TransactionAttribute;

#[derive(Default)]
struct Managers {
    default: Option<Arc<dyn PlatformTransactionManager>>,
    named: HashMap<String, Arc<dyn PlatformTransactionManager>>,
}

#[derive(Default)]
pub struct TransactionManagerRegistry {
    managers: RwLock<Managers>,
}

impl TransactionManagerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_default(&self, manager: Arc<dyn PlatformTransactionManager>) {
        self.managers.write().expect("registry lock").default = Some(manager);
    }

    pub fn register(&self, qualifier: impl Into<String>, manager: Arc<dyn PlatformTransactionManager>) {
        self.managers.write().expect("registry lock").named.insert(qualifier.into(), manager);
    }

    pub fn determine_transaction_manager(
        &self,
        attribute: &TransactionAttribute,
    ) -> Result<Arc<dyn PlatformTransactionManager>, TransactionError> {
        let managers = self.managers.read().expect("registry lock");
        if let Some(qualifier) = attribute.qualifier.as_deref().filter(|qualifier| !qualifier.trim().is_empty()) {
            return managers.named.get(qualifier).cloned().ok_or_else(|| {
                TransactionError::InvalidState(format!(
                    "No transaction manager is registered with qualifier '{qualifier}'. Register it with \
                     demarcate_interceptor::registry().register(\"{qualifier}\", manager) before the first \
                     #[transactional(transaction_manager = \"{qualifier}\")] call"
                ))
            });
        }
        managers.default.clone().ok_or_else(|| {
            TransactionError::InvalidState(
                "No default transaction manager is registered. Call \
                 demarcate_interceptor::registry().set_default(manager) at application startup, or name one \
                 with #[transactional(transaction_manager = \"...\")]"
                    .into(),
            )
        })
    }
}

pub fn registry() -> &'static TransactionManagerRegistry {
    static REGISTRY: OnceLock<TransactionManagerRegistry> = OnceLock::new();
    REGISTRY.get_or_init(TransactionManagerRegistry::new)
}
