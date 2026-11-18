# demarcate

Declarative transactions for Rust, ported from the Spring Framework.

Put `#[transactional]` on a service function. Everything it calls joins the
same transaction, without passing a transaction handle through the layers.

```rust
use demarcate::transactional;

#[derive(Debug)]
enum Error {
    Transaction(demarcate::TransactionError),
    Sql(sqlx::Error),
}

impl From<demarcate::TransactionError> for Error {
    fn from(error: demarcate::TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self::Sql(error)
    }
}

struct Orders {
    pool: demarcate::postgres::TransactionalPool,
}

impl Orders {
    #[transactional]
    pub async fn place(&self, item: &str) -> Result<(), Error> {
        self.insert(item).await?;
        self.audit("placed").await
    }

    #[transactional(propagation = RequiresNew)]
    async fn audit(&self, event: &str) -> Result<(), Error> {
        sqlx::query("insert into audit values($1)").bind(event.to_owned()).execute(&self.pool).await?;
        Ok(())
    }

    async fn insert(&self, item: &str) -> Result<(), Error> {
        sqlx::query("insert into orders values($1)").bind(item.to_owned()).execute(&self.pool).await?;
        Ok(())
    }
}
```

`insert` has no transaction code. `TransactionalPool` implements
`sqlx::Executor` and runs each query on the connection of the current
transaction. `audit` runs in its own transaction and commits even when
`place` rolls back.

## Status

Version 0.1 is the first release. The API can change before 1.0. Only
PostgreSQL through sqlx is implemented.

## Setup

demarcate is not on crates.io yet. Depend on a release tag of this
repository:

```toml
[dependencies]
demarcate = { git = "https://github.com/HarryJhin/demarcate", tag = "v0.1.0", features = ["sqlx-postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres"] }
```

The minimum Rust version is 1.94, which sqlx 0.9 requires.

Register a transaction manager once at startup:

```rust
use std::sync::Arc;
use demarcate::postgres::{TransactionalPool, manager};

let pg_pool = sqlx::PgPool::connect(&database_url).await?;
let pool = TransactionalPool::new(pg_pool);
demarcate::registry().set_default(Arc::new(manager(pool.clone())));
```

A `#[transactional]` function is an `async fn` that returns `Result<T, E>`
where `E: From<demarcate::TransactionError>`, because beginning, committing
and rolling back can fail.

On an `impl` block the attribute applies to every `async fn` in the block,
and a declaration on a method overrides it, as with a class-level
`@Transactional`. A method that does not return such a `Result` is a
compile error, so no method is left out without a word. Methods that are
not transactional go in a separate `impl` block without the attribute.

## Examples

| Example | Content |
|:--|:--|
| `examples/quickstart` | the service above as a runnable program in one file |
| `examples/axum-orders` | an order API with axum, sqlx and PostgreSQL in handlers, services and repositories |

`axum-orders` shows the transaction boundaries of a layered application:
a use case in one transaction, an audit entry that outlives a rollback
(`RequiresNew`), a step that may fail without failing the use case
(`Nested`), a confirmation sent after the commit, and repositories without
transaction code. Its README lists each requirement and the function that
solves it.

## What comes from Spring

The behaviour follows Spring Framework 7.0.10 (`spring-tx`).

| Spring | demarcate |
|:--|:--|
| `@Transactional` | `#[transactional]` on an `async fn` or an `impl` block |
| `propagation`, `isolation`, `timeout`, `readOnly` | `propagation = RequiresNew`, `isolation = Serializable`, `timeout = 30`, `read_only` |
| `rollbackFor`, `noRollbackFor` | `rollback_for = [E]`, `no_rollback_for = [E]`, `rollback_if = f`, `no_rollback_if = f` |
| `PlatformTransactionManager`, `TransactionStatus` | traits of the same name |
| `TransactionTemplate` | `TransactionTemplate::execute` |
| `TransactionSynchronization` | trait of the same name, async |
| `@TransactionalEventListener` | `on_transaction(TransactionPhase::AfterCommit, ..)` |
| `TransactionAspectSupport.currentTransactionStatus()` | `current_transaction_status()` |

All seven propagation modes are implemented, including `Nested` with
savepoints.

## Where it differs from Spring

- State is bound to the call, not to a thread. Every `#[transactional]`
  call has its own transaction context, which its futures see while they
  are polled. A spawned task does not inherit it; open a new transaction
  inside the task. `demarcate::synchronization::spawn` spawns a Tokio
  task with an empty context.
- The default rule rolls back on every `Err` and on panic. Rust has no
  checked exceptions. This matches Spring's `rollbackOn = ALL_EXCEPTIONS`.
- The macro wraps the function itself, so calls on `self` are
  transactional too.
- A future can be dropped before it completes. The call is then rolled
  back when its future is dropped, also when something else still holds
  its `TransactionStatus`, and callbacks that have not started do not
  run. A call dropped while it commits may already be committed.
  Dropping a call that joined its caller's transaction, a `Nested` call
  included, marks that transaction rollback-only for good.
- Futures that one task runs beside each other (`join!`, `select!`, a
  stream combinator) keep the transaction of the function they were
  written in. Inside a transactional function, a query beside a nested
  call runs in that function's transaction, also while the nested call
  suspends it for a `RequiresNew` transaction. Two transactional calls
  beside each other at the top level are two transactions. The queries
  of one transaction still run one after another: awaiting two of them
  at once gives an error, not a second connection.
- A `Nested` transaction runs on a savepoint of the caller's connection.
  Until it completes, that connection belongs to the nested call. A
  future beside it that queries, or starts a nested transaction of its
  own, gets an error; if that future is a transactional call, its
  failure marks the transaction rollback-only as any failed call does.
  `RequiresNew` has no such limit.
- A call that joins its caller's transaction must finish before the
  caller does. Its queries get an error once the caller has returned.
  Do not keep such a future and poll it later.
- `TransactionStatus::create_savepoint` and the other savepoint methods
  are for use from one future at a time, like the direct manager calls
  below. Release or roll back a savepoint in the function that created
  it.
- PostgreSQL aborts a transaction when a statement in it fails, and a
  `COMMIT` of an aborted transaction rolls back without an error. When a
  statement run through `TransactionalPool` failed and the function
  still returns `Ok`, the commit first asks the server whether the
  transaction is usable and returns `UnexpectedRollback` if it is not.
  Run a statement that may fail in a `Nested` transaction, or roll back
  to a savepoint. A statement run on a connection taken with
  `TransactionalPool::acquire` is not tracked: if it fails and the
  function returns `Ok`, the transaction is rolled back and the function
  still returns `Ok`. Do not run `COMMIT` or `ROLLBACK` as SQL inside a
  transactional function.
- Data access in a listener or synchronization that runs after completion
  (`AfterCommit`, `AfterRollback`, `AfterCompletion`) joins a transaction
  that is rolled back when the callback phase ends, as Spring documents:
  it participates and is not committed. Run it in a `RequiresNew`
  transaction to keep it. A warning is logged when this happens.
- `PlatformTransactionManager::transaction`, `commit` and `rollback`
  called directly change the context they are called in, like the
  thread-bound state in Spring. Use them from one future at a time and
  complete transactions in reverse order of creation.
- `read_only` begins the transaction as `READ ONLY`, so PostgreSQL rejects
  a write in it. Spring only passes the flag to the JDBC driver.
- `timeout` is checked when a connection is used. A function that passes
  its deadline and runs no more queries still commits. A query after the
  deadline fails with `TimedOut`.
- Each nested `#[transactional]` call adds stack depth. A recursion of
  more than about a hundred transactional calls can overflow the stack
  of a Tokio worker thread in a debug build.
- JTA, XML configuration and the reactive variant are not ported.

## Checks that move to run time

Passing a transaction handle lets the compiler check that a function
runs inside a transaction. This library gives that up. To keep failures
loud:

- `pool.transaction_required()` returns a pool that refuses to run a
  query outside a transaction.
- `propagation = Mandatory` rejects a call without a caller transaction.
- Errors about a missing context, an unknown transaction manager or a
  connection that is already in use name the cause and the fix.
- An error raised inside a query through `TransactionalPool` arrives as
  `sqlx::Error::Configuration` that wraps the `TransactionError`, because
  the `sqlx::Executor` trait fixes the error type.

## Crates

| Crate | Content |
|:--|:--|
| `demarcate` | entry crate, re-exports the others |
| `demarcate-core` | definition, status, synchronization, transaction manager core, template; no async runtime required |
| `demarcate-interceptor` | transaction attribute, rollback rules, invocation flow, registry |
| `demarcate-macros` | `#[transactional]` |
| `demarcate-sqlx` | sqlx driver for PostgreSQL, `TransactionalPool` |

A driver for another database library implements `TransactionDriver`
from `demarcate-core`.

## Tests

```sh
cargo test
DEMARCATE_TEST_DATABASE_URL=postgres://... cargo test -- --include-ignored
```

The second command runs the PostgreSQL tests.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
