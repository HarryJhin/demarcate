use std::time::Duration;

/// How a transactional call relates to a transaction that is already running.
///
/// The mode is evaluated when the call starts. "Current transaction" means the transaction of the
/// caller, as seen through the transaction context of the calling future.
///
/// The default is [`Propagation::Required`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Propagation {
    /// Join the current transaction, or start a new one if there is none.
    ///
    /// A call that joins shares the fate of the transaction it joined: when the call fails, the whole
    /// transaction is marked rollback-only, and its caller cannot commit it.
    #[default]
    Required,
    /// Join the current transaction, or run without a transaction if there is none.
    ///
    /// Without a transaction the call still opens a synchronization scope, so a connection obtained
    /// inside it is reused until the call ends.
    Supports,
    /// Join the current transaction, and fail if there is none.
    ///
    /// Use this for a function that must never run outside a transaction.
    ///
    /// Fails with [`TransactionError::InvalidTransactionState`](crate::TransactionError::InvalidTransactionState)
    /// when no transaction is running.
    Mandatory,
    /// Start a new, independent transaction, and suspend the current one if there is one.
    ///
    /// The new transaction commits or rolls back on its own. Its outcome does not depend on the
    /// outcome of the suspended transaction, and the suspended transaction resumes when the call
    /// ends. A driver needs a second connection for the new transaction.
    RequiresNew,
    /// Run without a transaction, and suspend the current one if there is one.
    NotSupported,
    /// Run without a transaction, and fail if one is running.
    ///
    /// Fails with [`TransactionError::InvalidTransactionState`](crate::TransactionError::InvalidTransactionState)
    /// when a transaction is running.
    Never,
    /// Run in a nested transaction if a transaction is running, or behave like
    /// [`Propagation::Required`] if there is none.
    ///
    /// A nested transaction is a savepoint in the current transaction. When the nested call fails,
    /// only the work since the savepoint is rolled back and the caller can continue. When the outer
    /// transaction rolls back, the nested work is rolled back with it.
    ///
    /// The transaction manager must allow nested transactions; see
    /// [`ManagerSettings::nested_transaction_allowed`](crate::ManagerSettings::nested_transaction_allowed).
    Nested,
}

/// The isolation level of a transaction.
///
/// The level applies only when the call starts a new transaction, that is with
/// [`Propagation::Required`], [`Propagation::RequiresNew`] or [`Propagation::Nested`] without a
/// current transaction. A call that joins a transaction keeps the level of that transaction.
///
/// The default is [`Isolation::Default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Isolation {
    /// Use the default level of the database.
    #[default]
    Default,
    /// Dirty reads, non-repeatable reads and phantom reads can occur.
    ReadUncommitted,
    /// Dirty reads are prevented; non-repeatable reads and phantom reads can occur.
    ReadCommitted,
    /// Dirty reads and non-repeatable reads are prevented; phantom reads can occur.
    RepeatableRead,
    /// Dirty reads, non-repeatable reads and phantom reads are prevented.
    Serializable,
}

/// The properties of a transaction: propagation, isolation, timeout, read-only flag and name.
///
/// A definition is passed to
/// [`PlatformTransactionManager::transaction`](crate::PlatformTransactionManager::transaction).
/// The isolation, timeout and read-only settings apply only when the call starts a new
/// transaction; a call that joins an existing transaction keeps the settings of that transaction.
///
/// The default definition is [`Propagation::Required`], [`Isolation::Default`], no timeout,
/// read-write and unnamed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransactionDefinition {
    /// How the call relates to a transaction that is already running.
    pub propagation: Propagation,
    /// The isolation level of a transaction that the call starts.
    pub isolation: Isolation,
    /// The time after which a transaction that the call starts must not run further statements.
    ///
    /// `None` uses the default timeout of the transaction manager.
    pub timeout: Option<Duration>,
    /// Whether a transaction that the call starts is read-only.
    ///
    /// This is a hint for the driver. A driver that cannot apply it ignores it.
    pub read_only: bool,
    /// The name of the transaction, shown in logs and exposed as
    /// [`synchronization::current_transaction_name`](crate::synchronization::current_transaction_name).
    pub name: Option<String>,
}

impl TransactionDefinition {
    /// Creates a definition with the default settings.
    pub fn new() -> Self {
        Self::default()
    }
    /// Creates a definition with the given propagation and the default for everything else.
    pub fn with_propagation(propagation: Propagation) -> Self {
        Self { propagation, ..Self::default() }
    }
    /// Sets the propagation.
    pub fn propagation(mut self, propagation: Propagation) -> Self {
        self.propagation = propagation;
        self
    }
    /// Sets the isolation level.
    pub fn isolation(mut self, isolation: Isolation) -> Self {
        self.isolation = isolation;
        self
    }
    /// Sets the timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    /// Sets the read-only flag.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    /// Sets the name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}
