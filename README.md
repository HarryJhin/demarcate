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

Version 0.1 is the first release. The API can change before 1.0. There
are drivers for sqlx, SeaORM, tokio-postgres and diesel-async.

## Setup

demarcate is not on crates.io yet. Depend on a release tag of this
repository:

```toml
[dependencies]
demarcate = { git = "https://github.com/HarryJhin/demarcate", tag = "v0.1.0", features = ["sqlx-postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres"] }
```

## Compatibility

| | Supported | Checked by the CI |
|:--|:--|:--|
| Rust | 1.94 and later; sqlx 0.9 requires 1.94 | 1.94 and the current stable release |
| sqlx | 0.9.1 and later 0.9 releases | 0.9.1, and weekly the newest release |
| SeaORM | 2.0.4 and later 2 releases | 2.0.4, and weekly the newest release |
| tokio-postgres, deadpool-postgres | 0.7.13 and later 0.7 releases; 0.14.1 and later 0.14 releases | those versions, and weekly the newest releases |
| diesel-async, diesel | 0.10 releases; 2.4 and later 2 releases | 0.10.0 and 2.4.0, and weekly the newest releases |
| tokio | 1.38 and later | 1.38, and weekly the newest release |
| PostgreSQL | not limited | 18 |
| MySQL | not limited | 8.4 |
| MariaDB | not limited | 11 |
| SQLite | the version that sqlx bundles | that version |

One CI job builds and tests with the lowest version of every direct
dependency that `Cargo.toml` allows. A weekly job does the same with the
newest versions.

The same conformance tests run on every database. Where a database behaves
differently, the tests assert that behaviour, and the section "Drivers"
lists it. Every driver runs these tests on each of its databases.

## Setup

demarcate is not on crates.io yet. Depend on a release tag of this
repository:

```toml
[dependencies]
demarcate = { git = "https://github.com/HarryJhin/demarcate", tag = "v0.1.0", features = ["sqlx-postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres"] }
```

## Compatibility

| | Supported | Checked by the CI |
|:--|:--|:--|
| Rust | 1.94 and later; sqlx 0.9 requires 1.94 | 1.94 and the current stable release |
| sqlx | 0.9.1 and later 0.9 releases | 0.9.1, and weekly the newest release |
| tokio | 1.38 and later | 1.38, and weekly the newest release |
| PostgreSQL | not limited | 18 |

One CI job builds and tests with the lowest version of every direct
dependency that `Cargo.toml` allows. A weekly job does the same with the
newest versions.

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

## Drivers

Enable one feature for each connection library and database. Every driver
module has a transaction-aware pool, a `manager` function and the same
behaviour; the pool offers the query API of its library.

| Feature | Module | Library | Databases | Data access |
|:--|:--|:--|:--|:--|
| `sqlx-postgres` | `demarcate::postgres` | sqlx | PostgreSQL | `sqlx::query(..).execute(&pool)` |
| `sqlx-mysql` | `demarcate::mysql` | sqlx | MySQL, MariaDB | `sqlx::query(..).execute(&pool)` |
| `sqlx-sqlite` | `demarcate::sqlite` | sqlx | SQLite | `sqlx::query(..).execute(&pool)` |
| `sea-orm` | `demarcate::sea_orm` | SeaORM | PostgreSQL, MySQL, MariaDB, SQLite | `Entity::find().all(&database)` |
| `tokio-postgres` | `demarcate::tokio_postgres` | tokio-postgres on deadpool-postgres | PostgreSQL | `pool.client().await?.query(..)` |
| `diesel-async-postgres` | `demarcate::diesel_async::postgres` | diesel-async on deadpool | PostgreSQL | `query.load(&mut *pool.connection().await?)` |
| `diesel-async-mysql` | `demarcate::diesel_async::mysql` | diesel-async on deadpool | MySQL, MariaDB | `query.load(&mut *pool.connection().await?)` |

The sqlx and SeaORM drivers take the pool itself as the executor. The
tokio-postgres and diesel-async drivers hand out the connection of the
current transaction; drop it before the next request and before calling
another transactional function.

### Databases

| | PostgreSQL | MySQL, MariaDB | SQLite |
|:--|:--|:--|:--|
| `read_only` | the server rejects a write | the server rejects a write | not enforced |
| `isolation` | all four levels | all four levels | `Default` and `Serializable`; another level is an error |
| `timeout` | checked before each query, and the server cancels a running statement | checked before each query | checked before each query |
| A statement fails and the function returns `Ok` | the commit returns `UnexpectedRollback` | the other statements commit | the other statements commit |
| `RequiresNew` that writes after its caller wrote | commits on its own | commits on its own | fails with `database is locked` |
| Schema statements (`CREATE TABLE`) | are part of the transaction | commit the transaction on the server | are part of the transaction |

MySQL and MariaDB roll the whole transaction back when a statement fails
with a deadlock (SQLSTATE `40001`), and the connection then runs in
autocommit mode. The sqlx and SeaORM drivers see that error: every further
query of the transaction returns an error and does not run, and the
transaction cannot commit. The diesel-async driver does not see the errors
of statements, so it cannot detect a deadlock; return the error from the
transactional function.

The tokio-postgres and diesel-async drivers do not see a failed statement
on PostgreSQL either. Their commit asks the server whether the transaction
is usable, which costs one round trip.

SQLite allows one writer at a time. Use the WAL journal mode, so that
readers and the writer do not block each other.

### Another library

`demarcate-connection` holds the transaction logic that is the same for
every connection library. A driver implements its `ConnectionBackend`
trait: how to obtain a connection, begin, commit, roll back and run a
savepoint statement. Each driver above is about 300 lines. A resource that
is not a database connection implements `TransactionDriver` from
`demarcate-core`.

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
| `demarcate-connection` | the transaction driver for connection libraries: connection holder, savepoints, the `ConnectionBackend` trait |
| `demarcate-interceptor` | transaction attribute, rollback rules, invocation flow, registry |
| `demarcate-macros` | `#[transactional]` |
| `demarcate-sqlx` | sqlx drivers for PostgreSQL, MySQL, MariaDB and SQLite, `TransactionalPool` |
| `demarcate-sea-orm` | SeaORM driver, `TransactionalDatabase` |
| `demarcate-tokio-postgres` | tokio-postgres driver on deadpool-postgres |
| `demarcate-diesel-async` | diesel-async drivers on deadpool for PostgreSQL, MySQL and MariaDB |

## Tests

```sh
cargo test
DEMARCATE_TEST_DATABASE_URL=postgres://... \
DEMARCATE_TEST_MYSQL_URL=mysql://... \
DEMARCATE_TEST_MARIADB_URL=mysql://... \
  cargo test -- --include-ignored
```

The first command runs the tests that need no server, the SQLite tests
included. The second command also runs the PostgreSQL, MySQL and MariaDB
tests.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
