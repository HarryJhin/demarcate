//! The invocation flow behind `#[transactional]`: transaction attributes, rollback rules and
//! the registry of transaction managers.
//!
//! [`invoke_within_transaction`] runs a future in a transaction described by a
//! [`TransactionAttribute`]. It obtains the transaction from a manager in the [`registry`],
//! commits when the future returns `Ok`, and applies the [`RollbackRule`]s of the attribute
//! when it returns `Err`. The `#[transactional]` attribute macro generates a call to it.
//!
//! Applications normally depend on the `demarcate` crate, which re-exports these items.

#![warn(missing_docs)]

mod aspect;
mod attribute;
mod registry;

pub use aspect::{
    TransactionInfo, current_transaction_info, current_transaction_status, invoke_with_manager,
    invoke_within_transaction,
};
#[doc(hidden)]
pub use aspect::{TransactionalReturn, assert_transactional_return};
pub use attribute::{RollbackRule, TransactionAttribute, error_chain_contains};
pub use registry::{MethodRollbackEvent, TransactionManagerRegistry, registry};
