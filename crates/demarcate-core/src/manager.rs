use std::sync::Arc;

use crate::BoxFuture;
use std::time::Duration;

use crate::definition::{Isolation, Propagation, TransactionDefinition};
use crate::driver::TransactionDriver;
use crate::error::TransactionError;
use crate::listener::TransactionExecutionListener;
use crate::status::{ManagedTransactionStatus, TransactionExecution, TransactionStatus};
use crate::synchronization::{self, CompletionStatus, TransactionSynchronization};

/// Selects the calls for which a [`TransactionManager`] opens a synchronization scope, in which
/// [`TransactionSynchronization`] callbacks can be registered.
///
/// The default is [`SynchronizationMode::Always`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SynchronizationMode {
    /// Opens a scope for every call, including one that runs without a transaction.
    #[default]
    Always,
    /// Opens a scope only for a call that runs in an actual transaction.
    OnActualTransaction,
    /// Opens no scope. Synchronization callbacks cannot be registered.
    Never,
}

/// The central interface of the transaction infrastructure: begin, commit and roll back.
///
/// Application code rarely calls this directly. Declare transactions with `#[transactional]`, or
/// run a closure with [`TransactionTemplate`](crate::TransactionTemplate); both call a manager and
/// give every call its own transaction context.
///
/// Direct calls change the context of the calling future in place. Use them from one future at a
/// time, inside [`synchronization::scope`], and complete transactions in the reverse order of
/// their creation.
pub trait PlatformTransactionManager: Send + Sync {
    /// Returns the status of a transaction for the given definition: an existing transaction that
    /// the call joins, or a new one, according to the propagation mode.
    ///
    /// `None` uses the default definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the propagation mode does not fit the current state, for example
    /// [`Propagation::Mandatory`] without a transaction, or when the transaction cannot be started.
    fn transaction<'a>(
        &'a self,
        definition: Option<&'a TransactionDefinition>,
    ) -> BoxFuture<'a, Result<Arc<dyn TransactionStatus>, TransactionError>>;

    /// Completes the transaction of the given status.
    ///
    /// When the status is marked rollback-only, the transaction is rolled back instead. When the
    /// call only joined the transaction, nothing is committed here; the call that started the
    /// transaction commits it. When the call suspended a transaction, that transaction resumes.
    ///
    /// After this returns, with or without an error, the status is completed and must not be passed
    /// to `commit` or `rollback` again.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::UnexpectedRollback`] when the transaction was rolled back because
    /// it was marked rollback-only by a call that joined it, and [`TransactionError::System`] when
    /// the commit failed. Returns the error of a `before_commit` callback after rolling back, and
    /// the error of an `after_commit` callback, in which case the transaction is committed. Returns
    /// [`TransactionError::InvalidTransactionState`] when the status is already completed.
    fn commit<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>>;

    /// Rolls back the transaction of the given status.
    ///
    /// When the call only joined the transaction, the transaction is marked rollback-only and the
    /// call that started it rolls it back. When the call suspended a transaction, that transaction
    /// resumes.
    ///
    /// Do not call this after `commit` returned an error; the transaction is already completed.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::InvalidTransactionState`] when the status is already completed,
    /// and the error of the driver when the rollback fails.
    fn rollback<'a>(&'a self, status: &'a dyn TransactionStatus) -> BoxFuture<'a, Result<(), TransactionError>>;
}

/// The resources and synchronizations of a suspended transaction, kept in the status of the call
/// that suspended it.
pub struct SuspendedResourcesHolder<D: TransactionDriver> {
    suspended_resources: Option<D::Suspended>,
    suspended_synchronizations: Option<Vec<Arc<dyn TransactionSynchronization>>>,
    name: Option<String>,
    read_only: bool,
    isolation_level: Option<Isolation>,
    was_active: bool,
}

/// The settings of a [`TransactionManager`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagerSettings {
    /// When the manager opens a synchronization scope. The default is
    /// [`SynchronizationMode::Always`].
    pub transaction_synchronization: SynchronizationMode,
    /// The timeout of a transaction whose definition has none. The default is `None`: no timeout.
    pub default_timeout: Option<Duration>,
    /// Whether [`Propagation::Nested`] is allowed. The default is `false`.
    pub nested_transaction_allowed: bool,
    /// Whether a call that joins a transaction fails when its definition asks for another isolation
    /// level, or for a read-write transaction while the existing one is read-only.
    ///
    /// The default is `false`: the settings of the joining call are ignored.
    pub validate_existing_transaction: bool,
    /// Whether a call that joined a transaction and fails marks the whole transaction rollback-only.
    ///
    /// The default is `true`. With `false`, the call that started the transaction decides, and can
    /// commit after a joined call failed. The setting does not apply to
    /// [`set_rollback_only`](crate::TransactionExecution::set_rollback_only): a joined call that
    /// calls it always marks the whole transaction.
    pub global_rollback_on_participation_failure: bool,
    /// Whether a call that joined a transaction gets
    /// [`TransactionError::UnexpectedRollback`] at its own end when the transaction is marked
    /// rollback-only, instead of only the call that started the transaction.
    ///
    /// The default is `false`.
    pub fail_early_on_global_rollback_only: bool,
    /// Whether the manager calls `rollback` on the driver when the commit failed.
    ///
    /// The default is `false`: the transaction is assumed to be completed by the failed commit.
    pub rollback_on_commit_failure: bool,
}

impl Default for ManagerSettings {
    fn default() -> Self {
        Self {
            transaction_synchronization: SynchronizationMode::Always,
            default_timeout: None,
            nested_transaction_allowed: false,
            validate_existing_transaction: false,
            global_rollback_on_participation_failure: true,
            fail_early_on_global_rollback_only: false,
            rollback_on_commit_failure: false,
        }
    }
}

/// A transaction manager that implements the propagation rules on top of a [`TransactionDriver`].
///
/// It decides from the definition and the current state whether to join, start, suspend or nest
/// a transaction, runs the completion sequence, and triggers the
/// [`TransactionSynchronization`] and
/// [`TransactionExecutionListener`] callbacks. The driver supplies the steps that depend on the
/// resource.
pub struct TransactionManager<D: TransactionDriver> {
    driver: Arc<D>,
    settings: ManagerSettings,
    listeners: Vec<Arc<dyn TransactionExecutionListener>>,
}

type Status<D> = Arc<ManagedTransactionStatus<D>>;

impl<D: TransactionDriver> TransactionManager<D> {
    /// Creates a manager with the default settings.
    pub fn new(driver: D) -> Self {
        Self::with_settings(driver, ManagerSettings::default())
    }

    /// Creates a manager with the given settings.
    pub fn with_settings(driver: D, settings: ManagerSettings) -> Self {
        Self { driver: Arc::new(driver), settings, listeners: Vec::new() }
    }

    /// Returns the driver.
    pub fn driver(&self) -> &Arc<D> {
        &self.driver
    }
    /// Returns the settings.
    pub fn settings(&self) -> &ManagerSettings {
        &self.settings
    }
    /// Returns the settings for modification.
    pub fn settings_mut(&mut self) -> &mut ManagerSettings {
        &mut self.settings
    }
    /// Registers a listener for the begin, commit and rollback steps of every transaction of this
    /// manager.
    pub fn add_listener(&mut self, listener: Arc<dyn TransactionExecutionListener>) {
        self.listeners.push(listener);
    }
    /// Returns the registered listeners.
    pub fn listeners(&self) -> &[Arc<dyn TransactionExecutionListener>] {
        &self.listeners
    }

    /// Returns the timeout that applies to the given definition: its own, or the default of this
    /// manager.
    pub fn determine_timeout(&self, definition: &TransactionDefinition) -> Option<Duration> {
        definition.timeout.or(self.settings.default_timeout)
    }

    fn notify(&self, f: impl Fn(&dyn TransactionExecutionListener)) {
        for listener in &self.listeners {
            f(listener.as_ref());
        }
    }

    /// Returns the status of a transaction for the given definition, with the concrete status type.
    ///
    /// See [`PlatformTransactionManager::transaction`].
    pub async fn status(&self, definition: Option<&TransactionDefinition>) -> Result<Status<D>, TransactionError> {
        let defaults = TransactionDefinition::default();
        let def = definition.unwrap_or(&defaults);
        let transaction = self.driver.transaction(&self.settings)?;
        let existing = self.driver.is_existing_transaction(&transaction);
        let frame = if self.changes_context(def, existing) { synchronization::detach() } else { None };
        let status = self.status_on_context(def, transaction, existing).await?;
        if frame.is_none()
            && synchronization::is_context_active()
            && (status.is_new_transaction() || status.is_new_synchronization() || status.has_suspended_resources())
        {
            status.set_completed();
            return Err(TransactionError::InvalidState(
                "internal error: a transaction that changes the context was created on a shared context".into(),
            ));
        }
        status.set_frame(frame);
        Ok(status)
    }

    fn changes_context(&self, definition: &TransactionDefinition, existing: bool) -> bool {
        let mode = self.settings.transaction_synchronization;
        let synchronization_active = synchronization::is_synchronization_active();
        if existing {
            match definition.propagation {
                Propagation::Never => false,
                Propagation::NotSupported | Propagation::RequiresNew => true,
                Propagation::Nested if self.settings.nested_transaction_allowed => {
                    !self.driver.use_savepoint_for_nested_transaction()
                }
                Propagation::Nested => false,
                _ => mode != SynchronizationMode::Never && !synchronization_active,
            }
        } else {
            match definition.propagation {
                Propagation::Mandatory => false,
                Propagation::Required | Propagation::RequiresNew | Propagation::Nested => true,
                _ => mode == SynchronizationMode::Always && !synchronization_active,
            }
        }
    }

    async fn status_on_context(
        &self,
        def: &TransactionDefinition,
        transaction: D::Transaction,
        existing: bool,
    ) -> Result<Status<D>, TransactionError> {
        if existing {
            return self.handle_existing_transaction(def, transaction).await;
        }

        match def.propagation {
            Propagation::Mandatory => Err(TransactionError::InvalidTransactionState(
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
                if def.isolation != Isolation::Default {
                    tracing::warn!(
                        "Custom isolation level specified but no actual transaction initiated; \
                         isolation level will effectively be ignored"
                    );
                }
                let new_synchronization = self.settings.transaction_synchronization == SynchronizationMode::Always;
                self.prepare_transaction_status(def, None, true, new_synchronization, None)
            }
        }
    }

    async fn handle_existing_transaction(
        &self,
        definition: &TransactionDefinition,
        transaction: D::Transaction,
    ) -> Result<Status<D>, TransactionError> {
        match definition.propagation {
            Propagation::Never => {
                return Err(TransactionError::InvalidTransactionState(
                    "Existing transaction found for transaction marked with propagation 'never'".into(),
                ));
            }
            Propagation::NotSupported => {
                let suspended = self.suspend(Some(&transaction)).await?;
                let new_synchronization = self.settings.transaction_synchronization == SynchronizationMode::Always;
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
                                "The error of the inner begin is replaced by an error of resuming the outer transaction"
                            );
                            return Err(resume_error);
                        }
                        Err(begin_error)
                    }
                };
            }
            Propagation::Nested => {
                if !self.settings.nested_transaction_allowed {
                    return Err(TransactionError::NestedNotSupported(
                        "Transaction manager does not allow nested transactions by default - \
                         set ManagerSettings::nested_transaction_allowed to true"
                            .into(),
                    ));
                }
                if self.driver.use_savepoint_for_nested_transaction() {
                    let status = self.new_transaction_status(definition, Some(transaction), false, false, true, None);
                    self.notify(|listener| listener.before_begin(status.as_ref()));
                    if let Err(error) = status.create_and_hold_savepoint().await {
                        self.notify(|listener| listener.after_begin(status.as_ref(), Some(&error)));
                        status.set_completed();
                        return Err(error);
                    }
                    self.notify(|listener| listener.after_begin(status.as_ref(), None));
                    return Ok(status);
                }
                return self
                    .start_transaction(definition, transaction, true, None)
                    .await
                    .map_err(|(error, _, _)| error);
            }
            _ => {}
        }

        if self.settings.validate_existing_transaction {
            if definition.isolation != Isolation::Default {
                let current = synchronization::current_transaction_isolation_level();
                if current != Some(definition.isolation) {
                    return Err(TransactionError::InvalidTransactionState(format!(
                        "A call that joins the existing transaction asked for isolation level {:?}, but the \
                         transaction runs at {}",
                        definition.isolation,
                        current.map_or_else(|| "an unknown isolation level".to_owned(), |level| format!("{level:?}"))
                    )));
                }
            }
            if !definition.read_only && synchronization::is_current_transaction_read_only() {
                return Err(TransactionError::InvalidTransactionState(
                    "A call that is not read-only cannot join the existing transaction, because the transaction is read-only"
                        .into(),
                ));
            }
        }
        let new_synchronization = self.settings.transaction_synchronization != SynchronizationMode::Never;
        self.prepare_transaction_status(definition, Some(transaction), false, new_synchronization, None)
    }

    async fn start_transaction(
        &self,
        definition: &TransactionDefinition,
        transaction: D::Transaction,
        nested: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Result<Status<D>, (TransactionError, Option<SuspendedResourcesHolder<D>>, Status<D>)> {
        let new_synchronization = self.settings.transaction_synchronization != SynchronizationMode::Never;
        let status =
            self.new_transaction_status(definition, Some(transaction), true, new_synchronization, nested, suspended);
        self.notify(|listener| listener.before_begin(status.as_ref()));
        let begun =
            self.driver.begin(status.transaction().expect("transaction present"), definition, &self.settings).await;
        if let Err(error) = begun {
            self.notify(|listener| listener.after_begin(status.as_ref(), Some(&error)));
            let suspended = status.take_suspended_resources();
            status.set_completed();
            return Err((error, suspended, status));
        }
        if let Err(error) = self.prepare_synchronization(&status, definition) {
            let suspended = status.take_suspended_resources();
            status.set_completed();
            return Err((error, suspended, status));
        }
        self.notify(|listener| listener.after_begin(status.as_ref(), None));
        Ok(status)
    }

    fn prepare_transaction_status(
        &self,
        definition: &TransactionDefinition,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Result<Status<D>, TransactionError> {
        let status = self.new_transaction_status(
            definition,
            transaction,
            new_transaction,
            new_synchronization,
            false,
            suspended,
        );
        self.prepare_synchronization(&status, definition)?;
        Ok(status)
    }

    fn new_transaction_status(
        &self,
        definition: &TransactionDefinition,
        transaction: Option<D::Transaction>,
        new_transaction: bool,
        new_synchronization: bool,
        nested: bool,
        suspended: Option<SuspendedResourcesHolder<D>>,
    ) -> Status<D> {
        let actual_new_synchronization = new_synchronization && !synchronization::is_synchronization_active();
        Arc::new(ManagedTransactionStatus::new(
            self.driver.clone(),
            definition.name.clone(),
            transaction,
            new_transaction,
            actual_new_synchronization,
            nested,
            definition.read_only,
            suspended,
            self.settings.global_rollback_on_participation_failure,
        ))
    }

    fn prepare_synchronization(
        &self,
        status: &ManagedTransactionStatus<D>,
        definition: &TransactionDefinition,
    ) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            synchronization::init_synchronization()?;
            synchronization::set_actual_transaction_active(status.has_transaction());
            synchronization::set_current_transaction_isolation_level(
                (definition.isolation != Isolation::Default).then_some(definition.isolation),
            );
            synchronization::set_current_transaction_read_only(definition.read_only);
            synchronization::set_current_transaction_name(definition.name.clone());
        }
        Ok(())
    }

    async fn suspend(
        &self,
        transaction: Option<&D::Transaction>,
    ) -> Result<Option<SuspendedResourcesHolder<D>>, TransactionError> {
        if synchronization::is_synchronization_active() {
            let suspended_synchronizations = self.suspend_synchronizations().await?;
            let suspended_resources = match transaction {
                Some(transaction) => match self.driver.suspend(transaction).await {
                    Ok(resources) => Some(resources),
                    Err(error) => {
                        self.resume_synchronizations(suspended_synchronizations).await?;
                        return Err(error);
                    }
                },
                None => None,
            };
            let name = synchronization::current_transaction_name();
            synchronization::set_current_transaction_name(None);
            let read_only = synchronization::is_current_transaction_read_only();
            synchronization::set_current_transaction_read_only(false);
            let isolation_level = synchronization::current_transaction_isolation_level();
            synchronization::set_current_transaction_isolation_level(None);
            let was_active = synchronization::is_actual_transaction_active();
            synchronization::set_actual_transaction_active(false);
            Ok(Some(SuspendedResourcesHolder {
                suspended_resources,
                suspended_synchronizations: Some(suspended_synchronizations),
                name,
                read_only,
                isolation_level,
                was_active,
            }))
        } else if let Some(transaction) = transaction {
            let suspended_resources = self.driver.suspend(transaction).await?;
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
                self.driver.resume(transaction, suspended_resources).await?;
            }
            if let Some(suspended_synchronizations) = holder.suspended_synchronizations {
                synchronization::set_actual_transaction_active(holder.was_active);
                synchronization::set_current_transaction_isolation_level(holder.isolation_level);
                synchronization::set_current_transaction_read_only(holder.read_only);
                synchronization::set_current_transaction_name(holder.name);
                self.resume_synchronizations(suspended_synchronizations).await?;
            }
        }
        Ok(())
    }

    async fn suspend_synchronizations(&self) -> Result<Vec<Arc<dyn TransactionSynchronization>>, TransactionError> {
        let suspended_synchronizations = synchronization::synchronizations()?;
        for synchronization in &suspended_synchronizations {
            synchronization.suspend().await?;
        }
        synchronization::clear_synchronization()?;
        Ok(suspended_synchronizations)
    }

    async fn resume_synchronizations(
        &self,
        suspended_synchronizations: Vec<Arc<dyn TransactionSynchronization>>,
    ) -> Result<(), TransactionError> {
        synchronization::init_synchronization()?;
        for synchronization in suspended_synchronizations {
            synchronization.resume().await?;
            synchronization::register_synchronization(synchronization)?;
        }
        Ok(())
    }

    fn downcast<'a>(
        &self,
        status: &'a dyn TransactionStatus,
    ) -> Result<&'a ManagedTransactionStatus<D>, TransactionError> {
        status.as_any().downcast_ref::<ManagedTransactionStatus<D>>().ok_or_else(|| {
            TransactionError::InvalidTransactionState(
                "Transaction status was not created by this transaction manager".into(),
            )
        })
    }

    /// Completes the transaction of the given status. See [`PlatformTransactionManager::commit`].
    pub async fn commit_status(&self, status: &dyn TransactionStatus) -> Result<(), TransactionError> {
        if status.is_completed() {
            return Err(TransactionError::InvalidTransactionState(
                "Transaction is already completed - do not call commit or rollback more than once per transaction"
                    .into(),
            ));
        }
        let status = self.downcast(status)?;
        if !status.is_in_order() {
            self.process_rollback(status, false).await?;
            return Err(TransactionError::InvalidState(synchronization::OUT_OF_ORDER.into()));
        }
        if status.is_local_rollback_only() {
            return self.process_rollback(status, false).await;
        }
        if !self.driver.should_commit_on_global_rollback_only() && status.is_global_rollback_only() {
            return self.process_rollback(status, true).await;
        }
        self.process_commit(status).await
    }

    /// Rolls back the transaction of the given status. See [`PlatformTransactionManager::rollback`].
    pub async fn rollback_status(&self, status: &dyn TransactionStatus) -> Result<(), TransactionError> {
        if status.is_completed() {
            return Err(TransactionError::InvalidTransactionState(
                "Transaction is already completed - do not call commit or rollback more than once per transaction"
                    .into(),
            ));
        }
        let status = self.downcast(status)?;
        let in_order = status.is_in_order();
        self.process_rollback(status, false).await?;
        if !in_order {
            return Err(TransactionError::InvalidState(synchronization::OUT_OF_ORDER.into()));
        }
        Ok(())
    }

    async fn process_commit(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
        let result = self.process_commit_body(status).await;
        let cleanup = self.cleanup_after_completion(status).await;
        cleanup.and(result)
    }

    async fn process_commit_body(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
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
                self.notify(|listener| listener.before_commit(status));
                commit_listener_invoked = true;
                status.release_held_savepoint().await?;
            } else if status.is_new_transaction() {
                unexpected_rollback = status.is_global_rollback_only();
                self.notify(|listener| listener.before_commit(status));
                commit_listener_invoked = true;
                self.driver.commit(status.transaction().expect("transaction present")).await?;
            } else if self.settings.fail_early_on_global_rollback_only {
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
                self.notify(|listener| listener.after_rollback(status, None));
            } else if error.is_transaction_error() {
                if self.settings.rollback_on_commit_failure {
                    self.rollback_on_commit_error(status, &error).await?;
                } else {
                    self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
                    if commit_listener_invoked {
                        self.notify(|listener| listener.after_commit(status, Some(&error)));
                    }
                }
            } else {
                if !before_completion_invoked {
                    self.trigger_before_completion(status).await?;
                }
                self.rollback_on_commit_error(status, &error).await?;
            }
            return Err(error);
        }

        let after_commit = self.trigger_after_commit(status).await;
        self.trigger_after_completion(status, CompletionStatus::Committed).await?;
        if commit_listener_invoked {
            self.notify(|listener| listener.after_commit(status, None));
        }
        after_commit
    }

    async fn process_rollback(
        &self,
        status: &ManagedTransactionStatus<D>,
        unexpected: bool,
    ) -> Result<(), TransactionError> {
        let result = self.process_rollback_body(status, unexpected).await;
        let cleanup = self.cleanup_after_completion(status).await;
        cleanup.and(result)
    }

    async fn process_rollback_body(
        &self,
        status: &ManagedTransactionStatus<D>,
        unexpected: bool,
    ) -> Result<(), TransactionError> {
        let mut unexpected_rollback = unexpected;
        let mut rollback_listener_invoked = false;

        let attempt: Result<(), TransactionError> = async {
            self.trigger_before_completion(status).await?;
            if status.has_savepoint() {
                self.notify(|listener| listener.before_rollback(status));
                rollback_listener_invoked = true;
                if let Err(error) = status.rollback_to_held_savepoint().await {
                    if let Some(transaction) = status.transaction() {
                        self.driver.set_rollback_only(transaction)?;
                    }
                    return Err(error);
                }
            } else if status.is_new_transaction() {
                self.notify(|listener| listener.before_rollback(status));
                rollback_listener_invoked = true;
                self.driver.rollback(status.transaction().expect("transaction present")).await?;
            } else {
                if let Some(transaction) = status.transaction() {
                    if status.is_local_rollback_only() || self.settings.global_rollback_on_participation_failure {
                        self.driver.set_rollback_only(transaction)?;
                    }
                } else {
                    tracing::debug!("Should roll back transaction but cannot - no transaction available");
                }
                if !self.settings.fail_early_on_global_rollback_only {
                    unexpected_rollback = false;
                }
            }
            Ok(())
        }
        .await;

        if let Err(error) = attempt {
            self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
            if rollback_listener_invoked {
                self.notify(|listener| listener.after_rollback(status, Some(&error)));
            }
            return Err(error);
        }

        self.trigger_after_completion(status, CompletionStatus::RolledBack).await?;
        if rollback_listener_invoked {
            self.notify(|listener| listener.after_rollback(status, None));
        }
        if unexpected_rollback {
            return Err(TransactionError::UnexpectedRollback(
                "Transaction rolled back because it has been marked as rollback-only".into(),
            ));
        }
        Ok(())
    }

    async fn rollback_on_commit_error(
        &self,
        status: &ManagedTransactionStatus<D>,
        commit_error: &TransactionError,
    ) -> Result<(), TransactionError> {
        let attempt: Result<(), TransactionError> = async {
            if status.is_new_transaction() {
                self.driver.rollback(status.transaction().expect("transaction present")).await?;
            } else if let Some(transaction) = status.transaction()
                && self.settings.global_rollback_on_participation_failure
            {
                self.driver.set_rollback_only(transaction)?;
            }
            Ok(())
        }
        .await;
        if let Err(rollback_error) = attempt {
            tracing::error!(error = %commit_error, "The commit error is replaced by an error of the rollback");
            self.trigger_after_completion(status, CompletionStatus::Unknown).await?;
            self.notify(|listener| listener.after_rollback(status, Some(&rollback_error)));
            return Err(rollback_error);
        }
        self.trigger_after_completion(status, CompletionStatus::RolledBack).await?;
        self.notify(|listener| listener.after_rollback(status, None));
        Ok(())
    }

    async fn trigger_before_commit(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            synchronization::trigger_before_commit(status.is_read_only()).await?;
        }
        Ok(())
    }

    async fn trigger_before_completion(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            synchronization::trigger_before_completion().await?;
        }
        Ok(())
    }

    async fn trigger_after_commit(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            synchronization::trigger_after_commit().await?;
        }
        Ok(())
    }

    async fn trigger_after_completion(
        &self,
        status: &ManagedTransactionStatus<D>,
        completion_status: CompletionStatus,
    ) -> Result<(), TransactionError> {
        if status.is_new_synchronization() {
            let synchronizations = synchronization::synchronizations()?;
            synchronization::clear_synchronization()?;
            if !status.has_transaction() || status.is_new_transaction() {
                synchronization::invoke_after_completion(&synchronizations, completion_status).await;
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

    async fn cleanup_after_completion(&self, status: &ManagedTransactionStatus<D>) -> Result<(), TransactionError> {
        status.set_completed();
        if status.is_new_synchronization() {
            synchronization::clear();
        }
        if status.is_new_transaction() {
            self.driver.cleanup_after_completion(status.transaction().expect("transaction present")).await;
        }
        let resumed = match status.take_suspended_resources() {
            Some(suspended) => self.resume(status.transaction(), Some(suspended)).await,
            None => Ok(()),
        };
        status.release_frame();
        resumed
    }
}

impl<D: TransactionDriver> PlatformTransactionManager for TransactionManager<D> {
    fn transaction<'a>(
        &'a self,
        definition: Option<&'a TransactionDefinition>,
    ) -> BoxFuture<'a, Result<Arc<dyn TransactionStatus>, TransactionError>> {
        Box::pin(async move {
            let status: Arc<dyn TransactionStatus> = self.status(definition).await?;
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
