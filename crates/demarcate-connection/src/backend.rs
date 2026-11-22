use std::time::Duration;

use demarcate_core::{TransactionDefinition, TransactionError};

/// The operations of one connection library on one database.
///
/// [`ConnectionDriver`](crate::ConnectionDriver) and [`ConnectionSource`](crate::ConnectionSource)
/// call these functions; they hold the logic that is the same for every library.
///
/// Note that a [`Connection`](ConnectionBackend::Connection) can be dropped while it runs a
/// transaction: for example when a call is cancelled, when a new transaction cannot be bound to
/// the context, and after a callback used the connection of a completed transaction. Dropping it
/// must roll the transaction back or close the connection. It must not return a connection with
/// an open transaction to a pool.
pub trait ConnectionBackend: Send + Sync + 'static {
    /// One connection: either in autocommit mode or running a transaction.
    type Connection: Send + 'static;
    /// The error type of the library.
    type Error: std::error::Error + Send + Sync + 'static;

    /// A short name identifying the library and the database.
    const NAME: &'static str;

    /// The message of the [`TransactionError::UnexpectedRollback`] that a commit returns when
    /// [`is_usable`](ConnectionBackend::is_usable) reports that the transaction can no longer
    /// commit.
    const UNUSABLE_MESSAGE: &'static str;

    /// Checks the definition of a new transaction before [`begin`](ConnectionBackend::begin). The
    /// default accepts every definition.
    ///
    /// Note that `begin` is also called without this check, with the default definition, when a
    /// callback uses the connection of a completed transaction.
    ///
    /// # Errors
    ///
    /// Returns an error, for example [`TransactionError::InvalidIsolationLevel`], when the database
    /// cannot run a transaction with this definition.
    fn validate(&self, _definition: &TransactionDefinition) -> Result<(), TransactionError> {
        Ok(())
    }

    /// Obtains a connection in autocommit mode.
    fn acquire(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    /// Obtains a connection and begins a transaction on it with the isolation level and the
    /// read-only flag of the definition.
    fn begin(
        &self,
        definition: &TransactionDefinition,
    ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    /// Returns whether the connection runs a transaction.
    fn in_transaction(connection: &Self::Connection) -> bool;

    /// Commits the transaction of the connection and releases the connection.
    fn commit(&self, connection: Self::Connection) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Rolls the transaction of the connection back and releases the connection.
    fn rollback(&self, connection: Self::Connection) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Runs one statement without parameters and without a result: a savepoint statement.
    fn execute(
        &self,
        connection: &mut Self::Connection,
        statement: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Limits the statements that follow on the connection to the remaining time of the
    /// transaction. It is called each time a connection that runs a transaction with a deadline is
    /// handed out. The default does nothing; the deadline itself is checked in every case.
    fn apply_timeout(
        &self,
        _connection: &mut Self::Connection,
        _remaining: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Returns whether a statement that failed with this error can have left the transaction in
    /// a state in which it cannot commit. The driver then calls
    /// [`is_usable`](ConnectionBackend::is_usable) before the commit.
    fn taints_transaction(&self, _error: &Self::Error) -> bool {
        false
    }

    /// Returns whether the database rolled the whole transaction back when a statement failed
    /// with this error, so that the connection is no longer in a transaction. The default is
    /// `false`.
    ///
    /// The driver then refuses every further use of the connection in this transaction, because
    /// a statement would run and commit on its own, and the transaction cannot commit.
    fn ends_transaction(&self, _error: &Self::Error) -> bool {
        false
    }

    /// Returns whether the transaction of the connection can still commit. Called before a commit
    /// when a statement failed with an error for which
    /// [`taints_transaction`](ConnectionBackend::taints_transaction) is `true`, or when a savepoint
    /// statement failed with any error. The default is `true`.
    fn is_usable(&self, _connection: &mut Self::Connection) -> impl Future<Output = bool> + Send {
        async { true }
    }

    /// Returns whether a failure to release a savepoint can be ignored, for example because the
    /// savepoint no longer exists. The default is `false`.
    fn release_failure_is_ignorable(&self, _error: &Self::Error) -> bool {
        false
    }
}
