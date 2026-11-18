use std::fmt;

/// A boxed error that can be sent across threads.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error of the transaction infrastructure.
///
/// The variants name the step that failed. The message of a variant states the cause, and where
/// the fix is known, the fix. A variant with a `source` includes the message of the source in its
/// own [`Display`](std::fmt::Display) output.
///
/// An application error type implements `From<TransactionError>` so that a transactional
/// function can return a failure to begin, commit or roll back.
#[derive(Debug)]
pub enum TransactionError {
    /// A transaction could not be started, for example because no connection could be opened.
    CannotCreateTransaction {
        /// What failed.
        message: String,
        /// The error of the database or the driver.
        source: Option<BoxError>,
    },
    /// A connection could not be obtained outside of starting a transaction.
    CannotGetConnection {
        /// What failed.
        message: String,
        /// The error of the database or the driver.
        source: Option<BoxError>,
    },
    /// The existing transaction, or its absence, does not fit the requested definition, or a
    /// completed transaction was completed again.
    InvalidTransactionState(String),
    /// The requested isolation level is not supported.
    InvalidIsolationLevel(String),
    /// A nested transaction was requested and the transaction manager or the transaction does not
    /// support one.
    NestedNotSupported(String),
    /// An operation needs a transaction and none is bound to the calling future.
    NoTransaction(String),
    /// A transaction had to be suspended and the driver cannot suspend one.
    SuspensionNotSupported(String),
    /// The database or the driver failed during begin, commit, rollback or a savepoint operation.
    System {
        /// What failed.
        message: String,
        /// The error of the database or the driver.
        source: Option<BoxError>,
        /// The application error that was being handled when the transaction step failed.
        application: Option<BoxError>,
    },
    /// The deadline of the transaction passed.
    TimedOut(String),
    /// The transaction API was used in a way it does not support.
    Usage(String),
    /// A commit was requested and the transaction was rolled back instead.
    ///
    /// This happens when a call that joined the transaction failed and marked it rollback-only, and
    /// the caller then tried to commit.
    UnexpectedRollback(String),
    /// The transaction ended in a state that the transaction manager cannot determine.
    HeuristicCompletion(String),
    /// The transaction context was used in a state that does not allow the operation, for example
    /// without a context, or after the transaction that a future joined has completed.
    InvalidState(String),
    /// An error from a callback or a driver that is not one of the above.
    Other(BoxError),
}

impl TransactionError {
    /// Returns whether this error reports a failed transaction step, as opposed to a wrong use of
    /// the context or an error from a callback.
    ///
    /// The transaction manager uses this to decide how to complete a transaction whose commit
    /// failed.
    pub fn is_transaction_error(&self) -> bool {
        !matches!(self, Self::InvalidState(_) | Self::CannotGetConnection { .. } | Self::Other(_))
    }
    /// Creates a [`TransactionError::System`] with the given message and source.
    pub fn system(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::System { message: message.into(), source: Some(source.into()), application: None }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CannotCreateTransaction { message, source: Some(source) }
            | Self::CannotGetConnection { message, source: Some(source) }
            | Self::System { message, source: Some(source), .. } => write!(f, "{message}: {source}"),
            Self::CannotCreateTransaction { message, .. }
            | Self::CannotGetConnection { message, .. }
            | Self::InvalidTransactionState(message)
            | Self::InvalidIsolationLevel(message)
            | Self::NestedNotSupported(message)
            | Self::NoTransaction(message)
            | Self::SuspensionNotSupported(message)
            | Self::System { message, .. }
            | Self::TimedOut(message)
            | Self::Usage(message)
            | Self::UnexpectedRollback(message)
            | Self::HeuristicCompletion(message)
            | Self::InvalidState(message) => f.write_str(message),
            Self::Other(source) => source.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CannotCreateTransaction { source, .. }
            | Self::CannotGetConnection { source, .. }
            | Self::System { source, .. } => source.as_deref().map(|e| e as &(dyn std::error::Error + 'static)),
            Self::Other(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}
