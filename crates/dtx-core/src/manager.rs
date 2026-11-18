use std::sync::Arc;

use crate::BoxFuture;
use crate::definition::{Isolation, Propagation, StaticTransactionDefinition, TIMEOUT_DEFAULT, TransactionDefinition};
use crate::driver::TransactionDriver;
use crate::error::TransactionError;
use crate::listener::TransactionExecutionListener;
use crate::status::{DefaultTransactionStatus, TransactionExecution, TransactionStatus};
use crate::synchronization::{
    CompletionStatus, TransactionSynchronization, TransactionSynchronizationManager as Tsm,
    TransactionSynchronizationUtils as Utils,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionSynchronizationMode {
    #[default]
    Always,
    OnActualTransaction,
    Never,
}

pub trait PlatformTransactionManager: Send + Sync {
    fn get_transaction<'a>(
        &'a self,
        definition: Option<&'a dyn TransactionDefinition>,
    ) -> BoxFuture<'a, Result<Arc<dyn TransactionStatus>, TransactionError>>;

    fn commit<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>>;

    fn rollback<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>>;
}

pub struct SuspendedResourcesHolder<D: TransactionDriver> {
    suspended_resources: Option<D::Suspended>,
    suspended_synchronizations: Option<Vec<Arc<dyn TransactionSynchronization>>>,
    name: Option<String>,
    read_only: bool,
    isolation_level: Option<Isolation>,
    was_active: bool,
}

pub struct AbstractPlatformTransactionManager<D: TransactionDriver> {
    driver: Arc<D>,
    transaction_synchronization: TransactionSynchronizationMode,
    default_timeout: i32,
    nested_transaction_allowed: bool,
    validate_existing_transaction: bool,
    global_rollback_on_participation_failure: bool,
    fail_early_on_global_rollback_only: bool,
    rollback_on_commit_failure: bool,
    transaction_execution_listeners: Vec<Arc<dyn TransactionExecutionListener>>,
}

type Status<D> = Arc<DefaultTransactionStatus<D>>;

impl<D: TransactionDriver> AbstractPlatformTransactionManager<D> {
    pub fn new(driver: D) -> Self {
        Self::from_arc(Arc::new(driver))
    }

    pub fn from_arc(driver: Arc<D>) -> Self {
        Self {
            driver,
            transaction_synchronization: TransactionSynchronizationMode::Always,
            default_timeout: TIMEOUT_DEFAULT,
            nested_transaction_allowed: false,
            validate_existing_transaction: false,
            global_rollback_on_participation_failure: true,
            fail_early_on_global_rollback_only: false,
            rollback_on_commit_failure: false,
            transaction_execution_listeners: Vec::new(),
        }
    }

    pub fn driver(&self) -> &Arc<D> {
        &self.driver
    }
    pub fn set_transaction_synchronization(&mut self, mode: TransactionSynchronizationMode) {
        self.transaction_synchronization = mode;
    }
    pub fn transaction_synchronization(&self) -> TransactionSynchronizationMode {
        self.transaction_synchronization
    }
    pub fn set_default_timeout(&mut self, default_timeout: i32) -> Result<(), TransactionError> {
        if default_timeout < TIMEOUT_DEFAULT {
            return Err(TransactionError::InvalidTimeout {
                message: "Invalid default timeout".into(),
                timeout: default_timeout,
            });
        }
        self.default_timeout = default_timeout;
        Ok(())
    }
    pub fn default_timeout(&self) -> i32 {
        self.default_timeout
    }
    pub fn set_nested_transaction_allowed(&mut self, allowed: bool) {
        self.nested_transaction_allowed = allowed;
    }
    pub fn is_nested_transaction_allowed(&self) -> bool {
        self.nested_transaction_allowed
    }
    pub fn set_validate_existing_transaction(&mut self, validate: bool) {
        self.validate_existing_transaction = validate;
    }
    pub fn is_validate_existing_transaction(&self) -> bool {
        self.validate_existing_transaction
    }
    pub fn set_global_rollback_on_participation_failure(&mut self, value: bool) {
        self.global_rollback_on_participation_failure = value;
    }
    pub fn is_global_rollback_on_participation_failure(&self) -> bool {
        self.global_rollback_on_participation_failure
    }
    pub fn set_fail_early_on_global_rollback_only(&mut self, value: bool) {
        self.fail_early_on_global_rollback_only = value;
    }
    pub fn is_fail_early_on_global_rollback_only(&self) -> bool {
        self.fail_early_on_global_rollback_only
    }
    pub fn set_rollback_on_commit_failure(&mut self, value: bool) {
        self.rollback_on_commit_failure = value;
    }
    pub fn is_rollback_on_commit_failure(&self) -> bool {
        self.rollback_on_commit_failure
    }
    pub fn set_transaction_execution_listeners(&mut self, listeners: Vec<Arc<dyn TransactionExecutionListener>>) {
        self.transaction_execution_listeners = listeners;
    }
    pub fn add_listener(&mut self, listener: Arc<dyn TransactionExecutionListener>) {
        self.transaction_execution_listeners.push(listener);
    }
    pub fn transaction_execution_listeners(&self) -> &[Arc<dyn TransactionExecutionListener>] {
        &self.transaction_execution_listeners
    }

    pub fn determine_timeout(&self, definition: &dyn TransactionDefinition) -> i32 {
        if definition.timeout() != TIMEOUT_DEFAULT { definition.timeout() } else { self.default_timeout }
    }

    fn listeners(&self, f: impl Fn(&dyn TransactionExecutionListener)) {
        for listener in &self.transaction_execution_listeners {
            f(listener.as_ref());
        }
    }

    pub async fn get_status(
        &self,
        definition: Option<&dyn TransactionDefinition>,
    ) -> Result<Status<D>, TransactionError> {
        let defaults = StaticTransactionDefinition;
        let def: &dyn TransactionDefinition = definition.unwrap_or(&defaults);
        let transaction = self.driver.do_get_transaction()?;

        if self.driver.is_existing_transaction(&transaction) {
            return self.handle_existing_transaction(def, transaction).await;
        }

        if def.timeout() < TIMEOUT_DEFAULT {
            return Err(TransactionError::InvalidTimeout {
                message: "Invalid transaction timeout".into(),
                timeout: def.timeout(),
            });
        }

        match def.propagation_behavior() {
            Propagation::Mandatory => Err(TransactionError::IllegalTransactionState(
                "No existing transaction found for transaction marked with propagation 'mandatory'".into(),
            )),
            Propagation::Required | Propagation::RequiresNew | Propagation::Nested => {
                let suspended = self.suspend(None).await?;
                match self.start_transaction(def, transaction, false, suspended).await {
                    Ok(status) => Ok(status),
                    Err((error, suspended, _)) => {
                        self.resume(None, suspended).await?;
                        Err(error)
                    }
                }
            }
            _ => {
                if def.isolation_level() != Isolation::Default {
                    tracing::warn!(
                        "Custom isolation level specified but no actual transaction initiated; \
                         isolation level will effectively be ignored"
                    );
                }
                let new_synchronization = self.transaction_synchronization == TransactionSynchronizationMode::Always;
                self.prepare_transaction_status(def, None, true, new_synchronization, None)
            }
        }
    }

    async fn handle_existing_transaction(
        &self,
        definition: &dyn TransactionDefinition,
        transaction: D::Transaction,
    ) -> Result<Status<D>, TransactionError> {
        match definition.propagation_behavior() {
            Propagation::Never => {
                return Err(TransactionError::IllegalTransactionState(
                    "Existing transaction found for transaction marked with propagation 'never'".into(),
                ));
            }
            Propagation::NotSupported => {
                let suspended = self.suspend(Some(&transaction)).await?;
                let new_synchronization = self.transaction_synchronization == TransactionSynchronizationMode::Always;
                return self.prepare_transaction_status(definition, None, false, new_synchronization, suspended);
            }
            Propagation::RequiresNew => {
                let suspended = self.suspend(Some(&transaction)).await?;
                return match self.start_transaction(definition, transaction, false, suspended).await {
                    Ok(status) => Ok(status),
                    Err((begin_error, suspended, failed)) => {
                        if let Err(resume_error) = self.resume(failed.transaction(), suspended).await {
                            tracing::error!(
                                error = %begin_error,
                                "Inner transaction begin exception overridden by outer transaction resume exception"
                            );
                            return Err(resume_error);
                        }
                        Err(begin_error)
                    }
                };
            }
            Propagation::Nested => {
                if !self.nested_transaction_allowed {
                    return Err(TransactionError::NestedTransactionNotSupported(
                        "Transaction manager does not allow nested transactions by default - \
                         specify 'nestedTransactionAllowed' property with value 'true'"
                            .into(),
                    ));
                }
                if self.driver.use_savepoint_for_nested_transaction() {
                    let status = self.new_transaction_status(definition, Some(transaction), false, false, true, None);
                    self.listeners(|listener| listener.before_begin(status.as_ref()));
                    if let Err(error) = status.create_and_hold_savepoint().await {
                        self.listeners(|listener| listener.after_begin(status.as_ref(), Some(&error)));
                        status.set_completed();
                        return Err(error);
                    }
                    self.listeners(|listener| listener.after_begin(status.as_ref(), None));
                    return Ok(status);
                }
                return self.start_transaction(definition, transaction, true, None).await.map_err(|(error, _, _)| error);
            }
            _ => {}
        }

        if self.validate_existing_transaction {
            if definition.isolation_level() != Isolation::Default {
                let current = Tsm::current_transaction_isolation_level();
                if current != Some(definition.isolation_level()) {
                    return Err(TransactionError::IllegalTransactionState(format!(
                        "Participating transaction with definition specifies isolation level which is \
                         incompatible with existing transaction: {}",
                        current.map_or_else(|| "(unknown)".to_owned(), |level| format!("{level:?}"))
                    )));
                }
            }
            if !definition.is_read_only() && Tsm::is_current_transaction_read_only() {
                return Err(TransactionError::IllegalTransactionState(
                    "Participating transaction with definition is not marked as read-only but existing transaction is"
                        .into(),
                ));
            }
        }
        let new_synchronization = self.transaction_synchronization != TransactionSynchronizationMode::Never;
        self.prepare_transaction_status(definition, Some(transaction), false, new_synchronization, None)
    }

    async fn start_transaction(
        &self,
        definition: &dyn TransactionDefinition,
        transaction: D::Transaction,
        nested: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Result<Status<D>, (TransactionError, Option<SuspendedResourcesHolder<D>>, Status<D>)> {
        let new_synchronization = self.transaction_synchronization != TransactionSynchronizationMode::Never;
        let status =
            self.new_transaction_status(definition, Some(transaction), true, new_synchronization, nested, suspended);
        self.listeners(|listener| listener.before_begin(status.as_ref()));
        let begun = self.driver.do_begin(status.transaction().expect("transaction present"), definition).await;
        let prepared = match begun {
            Ok(()) => self.prepare_synchronization(&status, definition),
            Err(error) => Err(error),
        };
        if let Err(error) = prepared {
            self.listeners(|listener| listener.after_begin(status.as_ref(), Some(&error)));
            status.set_completed();
            let suspended = status.take_suspended_resources();
            return Err((error, suspended, status));
        }
        self.listeners(|listener| listener.after_begin(status.as_ref(), None));
        Ok(status)
    }

    fn prepare_transaction_status(
        &self,
        definition: &dyn TransactionDefinition,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Result<Status<D>, TransactionError> {
        let status =
            self.new_transaction_status(definition, transaction, new_transaction, new_synchronization, false, suspended);
        self.prepare_synchronization(&status, definition)?;
        Ok(status)
    }

    fn new_transaction_status(
        &self,
        definition: &dyn TransactionDefinition,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        nested: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Status<D> {
        let actual_new_synchronization = new_synchronization && !Tsm::is_synchronization_active();
        Arc::new(DefaultTransactionStatus::new(
            self.driver.clone(),
            definition.name().map(str::to_owned),
            transaction,
            new_transaction,
            actual_new_synchronization,
            nested,
            definition.is_read_only(),
            suspended,
        ))
    }

    fn prepare_synchronization(
        &self,
        status: &DefaultTransactionStatus<D>,
        definition: &dyn TransactionDefinition,
    ) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            Tsm::init_synchronization()?;
            Tsm::set_actual_transaction_active(status.has_transaction());
            Tsm::set_current_transaction_isolation_level(
                (definition.isolation_level() != Isolation::Default).then(|| definition.isolation_level()),
            );
            Tsm::set_current_transaction_read_only(definition.is_read_only());
            Tsm::set_current_transaction_name(definition.name().map(str::to_owned));
        }
        Ok(())
    }

    async fn suspend(
        &self,
        transaction: Option<&D::Transaction>,
    ) -> Result<Option<SuspendedResourcesHolder<D>>, TransactionError> {
        if Tsm::is_synchronization_active() {
            let suspended_synchronizations = self.do_suspend_synchronization().await?;
            let suspended_resources = match transaction {
                Some(transaction) => match self.driver.do_suspend(transaction).await {
                    Ok(resources) => Some(resources),
                    Err(error) => {
                        self.do_resume_synchronization(suspended_synchronizations).await?;
                        return Err(error);
                    }
                },
                None => None,
            };
            let name = Tsm::current_transaction_name();
            Tsm::set_current_transaction_name(None);
            let read_only = Tsm::is_current_transaction_read_only();
            Tsm::set_current_transaction_read_only(false);
            let isolation_level = Tsm::current_transaction_isolation_level();
            Tsm::set_current_transaction_isolation_level(None);
            let was_active = Tsm::is_actual_transaction_active();
            Tsm::set_actual_transaction_active(false);
            Ok(Some(SuspendedResourcesHolder {
                suspended_resources,
                suspended_synchronizations: Some(suspended_synchronizations),
                name,
                read_only,
                isolation_level,
                was_active,
            }))
        } else if let Some(transaction) = transaction {
            let suspended_resources = self.driver.do_suspend(transaction).await?;
            Ok(Some(SuspendedResourcesHolder {
                suspended_resources: Some(suspended_resources),
                suspended_synchronizations: None,
                name: None,
                read_only: false,
                isolation_level: None,
                was_active: false,
            }))
        } else {
            Ok(None)
        }
    }

    async fn resume(
        &self,
        transaction: Option<&D::Transaction>,
        holder: Option<SuspendedResourcesHolder<D>>,
    ) -> Result<(), TransactionError> {
        if let Some(holder) = holder {
            if let Some(suspended_resources) = holder.suspended_resources {
                self.driver.do_resume(transaction, suspended_resources).await?;
            }
            if let Some(suspended_synchronizations) = holder.suspended_synchronizations {
                Tsm::set_actual_transaction_active(holder.was_active);
                Tsm::set_current_transaction_isolation_level(holder.isolation_level);
                Tsm::set_current_transaction_read_only(holder.read_only);
                Tsm::set_current_transaction_name(holder.name);
                self.do_resume_synchronization(suspended_synchronizations).await?;
            }
        }
        Ok(())
    }

    async fn do_suspend_synchronization(&self) -> Result<Vec<Arc<dyn TransactionSynchronization>>, TransactionError> {
        let suspended_synchronizations = Tsm::get_synchronizations()?;
        for synchronization in &suspended_synchronizations {
            synchronization.suspend().await?;
        }
        Tsm::clear_synchronization()?;
        Ok(suspended_synchronizations)
    }

    async fn do_resume_synchronization(
        &self,
        suspended_synchronizations: Vec<Arc<dyn TransactionSynchronization>>,
    ) -> Result<(), TransactionError> {
        Tsm::init_synchronization()?;
        for synchronization in suspended_synchronizations {
            synchronization.resume().await?;
            Tsm::register_synchronization(synchronization)?;
        }
        Ok(())
    }

    fn downcast<'a>(&self, status: &'a dyn TransactionStatus) -> Result<&'a DefaultTransactionStatus<D>, TransactionError> {
        status.as_any().downcast_ref::<DefaultTransactionStatus<D>>().ok_or_else(|| {
            TransactionError::IllegalTransactionState("Transaction status was not created by this transaction manager".into())
        })
    }

    pub async fn commit_status(&self, status: &dyn TransactionStatus) -> Result<(), TransactionError> {
        if status.is_completed() {
            return Err(TransactionError::IllegalTransactionState(
                "Transaction is already completed - do not call commit or rollback more than once per transaction"
                    .into(),
            ));
        }
        let status = self.downcast(status)?;
        if status.is_local_rollback_only() {
            return self.process_rollback(status, false).await;
        }
        if !self.driver.should_commit_on_global_rollback_only() && status.is_global_rollback_only() {
            return self.process_rollback(status, true).await;
        }
        self.process_commit(status).await
    }

    pub async fn rollback_status(&self, status: &dyn TransactionStatus) -> Result<(), TransactionError> {
        if status.is_completed() {
            return Err(TransactionError::IllegalTransactionState(
                "Transaction is already completed - do not call commit or rollback more than once per transaction"
                    .into(),
            ));
        }
        let status = self.downcast(status)?;
        self.process_rollback(status, false).await
    }

    async fn process_commit(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        let result = self.process_commit_body(status).await;
        let cleanup = self.cleanup_after_completion(status).await;
        cleanup.and(result)
    }

    async fn process_commit_body(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        let mut before_completion_invoked = false;
        let mut commit_listener_invoked = false;

        let attempt: Result<(), TransactionError> = async {
            let unexpected_rollback;
            self.driver.prepare_for_commit(status.transaction()).await?;
            self.trigger_before_commit(status).await?;
            self.trigger_before_completion(status).await?;
            before_completion_invoked = true;

            if status.has_savepoint() {
                unexpected_rollback = status.is_global_rollback_only();
                self.listeners(|listener| listener.before_commit(status));
                commit_listener_invoked = true;
                status.release_held_savepoint().await?;
            } else if status.is_new_transaction() {
                unexpected_rollback = status.is_global_rollback_only();
                self.listeners(|listener| listener.before_commit(status));
                commit_listener_invoked = true;
                self.driver.do_commit(status.transaction().expect("transaction present")).await?;
            } else if self.fail_early_on_global_rollback_only {
                unexpected_rollback = status.is_global_rollback_only();
            } else {
                unexpected_rollback = false;
            }

            if unexpected_rollback {
                return Err(TransactionError::UnexpectedRollback(
                    "Transaction silently rolled back because it has been marked as rollback-only".into(),
                ));
            }
            Ok(())
        }
        .await;

        if let Err(error) = attempt {
            if matches!(error, TransactionError::UnexpectedRollback(_)) {
                self.trigger_after_completion(status, CompletionStatus::RolledBack).await?;
                self.listeners(|listener| listener.after_rollback(status, None));
            } else if error.is_transaction_exception() {
                if self.rollback_on_commit_failure {
                    self.do_rollback_on_commit_exception(status, &error).await?;
                } else {
                    self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
                    if commit_listener_invoked {
                        self.listeners(|listener| listener.after_commit(status, Some(&error)));
                    }
                }
            } else {
                if !before_completion_invoked {
                    self.trigger_before_completion(status).await?;
                }
                self.do_rollback_on_commit_exception(status, &error).await?;
            }
            return Err(error);
        }

        let after_commit = self.trigger_after_commit(status).await;
        self.trigger_after_completion(status, CompletionStatus::Committed).await?;
        if commit_listener_invoked {
            self.listeners(|listener| listener.after_commit(status, None));
        }
        after_commit
    }

    async fn process_rollback(
        &self,
        status: &DefaultTransactionStatus<D>,
        unexpected: bool,
    ) -> Result<(), TransactionError> {
        let result = self.process_rollback_body(status, unexpected).await;
        let cleanup = self.cleanup_after_completion(status).await;
        cleanup.and(result)
    }

    async fn process_rollback_body(
        &self,
        status: &DefaultTransactionStatus<D>,
        unexpected: bool,
    ) -> Result<(), TransactionError> {
        let mut unexpected_rollback = unexpected;
        let mut rollback_listener_invoked = false;

        let attempt: Result<(), TransactionError> = async {
            self.trigger_before_completion(status).await?;
            if status.has_savepoint() {
                self.listeners(|listener| listener.before_rollback(status));
                rollback_listener_invoked = true;
                status.rollback_to_held_savepoint().await?;
            } else if status.is_new_transaction() {
                self.listeners(|listener| listener.before_rollback(status));
                rollback_listener_invoked = true;
                self.driver.do_rollback(status.transaction().expect("transaction present")).await?;
            } else {
                if let Some(transaction) = status.transaction() {
                    if status.is_local_rollback_only() || self.global_rollback_on_participation_failure {
                        self.driver.do_set_rollback_only(transaction)?;
                    }
                } else {
                    tracing::debug!("Should roll back transaction but cannot - no transaction available");
                }
                if !self.fail_early_on_global_rollback_only {
                    unexpected_rollback = false;
                }
            }
            Ok(())
        }
        .await;

        if let Err(error) = attempt {
            self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
            if rollback_listener_invoked {
                self.listeners(|listener| listener.after_rollback(status, Some(&error)));
            }
            return Err(error);
        }

        self.trigger_after_completion(status, CompletionStatus::RolledBack).await?;
        if rollback_listener_invoked {
            self.listeners(|listener| listener.after_rollback(status, None));
        }
        if unexpected_rollback {
            return Err(TransactionError::UnexpectedRollback(
                "Transaction rolled back because it has been marked as rollback-only".into(),
            ));
        }
        Ok(())
    }

    async fn do_rollback_on_commit_exception(
        &self,
        status: &DefaultTransactionStatus<D>,
        commit_error: &TransactionError,
    ) -> Result<(), TransactionError> {
        let attempt: Result<(), TransactionError> = async {
            if status.is_new_transaction() {
                self.driver.do_rollback(status.transaction().expect("transaction present")).await?;
            } else if let Some(transaction) = status.transaction() {
                if self.global_rollback_on_participation_failure {
                    self.driver.do_set_rollback_only(transaction)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(rollback_error) = attempt {
            tracing::error!(error = %commit_error, "Commit exception overridden by rollback exception");
            self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
            self.listeners(|listener| listener.after_rollback(status, Some(&rollback_error)));
            return Err(rollback_error);
        }
        self.trigger_after_completion(status, CompletionStatus::RolledBack).await?;
        self.listeners(|listener| listener.after_rollback(status, None));
        Ok(())
    }

    async fn trigger_before_commit(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            Utils::trigger_before_commit(status.is_read_only()).await?;
        }
        Ok(())
    }

    async fn trigger_before_completion(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            Utils::trigger_before_completion().await?;
        }
        Ok(())
    }

    async fn trigger_after_commit(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            Utils::trigger_after_commit().await?;
        }
        Ok(())
    }

    async fn trigger_after_completion(
        &self,
        status: &DefaultTransactionStatus<D>,
        completion_status: CompletionStatus,
    ) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            let synchronizations = Tsm::get_synchronizations()?;
            Tsm::clear_synchronization()?;
            if !status.has_transaction() || status.is_new_transaction() {
                Utils::invoke_after_completion(&synchronizations, completion_status).await;
            } else if !synchronizations.is_empty() {
                self.driver
                    .register_after_completion_with_existing_transaction(
                        status.transaction().expect("transaction present"),
                        synchronizations,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn cleanup_after_completion(&self, status: &DefaultTransactionStatus<D>) -> Result<(), TransactionError> {
        status.set_completed();
        if status.is_new_synchronization() {
            Tsm::clear();
        }
        if status.is_new_transaction() {
            self.driver.do_cleanup_after_completion(status.transaction().expect("transaction present")).await;
        }
        if let Some(suspended) = status.take_suspended_resources() {
            self.resume(status.transaction(), Some(suspended)).await?;
        }
        Ok(())
    }
}

impl<D: TransactionDriver> PlatformTransactionManager for AbstractPlatformTransactionManager<D> {
    fn get_transaction<'a>(
        &'a self,
        definition: Option<&'a dyn TransactionDefinition>,
    ) -> BoxFuture<'a, Result<Arc<dyn TransactionStatus>, TransactionError>> {
        Box::pin(async move {
            let status: Arc<dyn TransactionStatus> = self.get_status(definition).await?;
            Ok(status)
        })
    }

    fn commit<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(self.commit_status(status))
    }

    fn rollback<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(self.rollback_status(status))
    }
}
