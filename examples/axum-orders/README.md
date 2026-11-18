# axum-orders

An order API with axum, sqlx and PostgreSQL, layered the usual way: HTTP
handlers call services, services call repositories. It shows where the
transaction boundaries of such an application go with demarcate.

| File | Content |
|:--|:--|
| `migrations/` | the schema and the demo data |
| `src/main.rs` | connects the pool, runs the migrations and starts the server |
| `src/lib.rs` | registers the transaction manager once and wires the services |
| `src/http.rs` | handlers; no transaction code |
| `src/service.rs` | the use cases; every `#[transactional]` of the application is here |
| `src/repository.rs` | SQL through `TransactionalPool`; no transaction code |
| `src/error.rs` | one error type, its `From<TransactionError>` and its HTTP responses |
| `src/model.rs` | the request and response types and the validation of an order |
| `src/notification.rs` | the channel that carries placed orders to the sender of confirmations |
| `tests/api.rs` | integration tests against PostgreSQL |

## What it shows

| Requirement | Solution | Where |
|:--|:--|:--|
| An order, its lines and the stock change together or not at all | `#[transactional]` on the use case; the repository queries run in its transaction | `OrderService::place_valid` |
| An attempt is recorded even when the order fails | `propagation = RequiresNew` | `AuditLog::record` |
| A coupon that cannot be used must not fail the order | `propagation = Nested`, and the caller handles the `Err` | `CouponService::redeem` |
| The confirmation goes out only for an order that is committed | `on_transaction(TransactionPhase::AfterCommit, ..)` | `OrderService::place_valid` |
| A read cannot write | `read_only` | `OrderService::find` |
| The transaction of an order runs no statement after 10 seconds | `timeout = 10`; it applies when the function starts the transaction | `OrderService::place_valid` |
| A repository is never used without a transaction | `TransactionalPool::transaction_required` | `repository.rs` |
| Bad input costs no transaction | validate first, then call the transactional function | `OrderService::place` |
| Concurrent orders for the same products do not deadlock | take the stock in the order of the SKU | `OrderService::place_valid` |
| The pool has a connection for every request | one limit for all routes: half the pool size | `src/main.rs` |

The tests cover every row except `read_only`, `timeout` and the request
limit.

## Run

```sh
docker run --rm -d --name orders-db -e POSTGRES_PASSWORD=orders -p 5432:5432 postgres:18
until docker exec orders-db pg_isready -q -h 127.0.0.1; do sleep 1; done
DATABASE_URL=postgres://postgres:orders@localhost/postgres cargo run -p demarcate-example-axum-orders
```

The server runs the migrations on start. They create the tables, two
products (`BOOK`, `PEN`) and one coupon (`WELCOME`).

```sh
# 201: the order is stored, and the log shows the confirmation
curl -X POST localhost:3000/orders -H 'content-type: application/json' \
  -d '{"customer":"kim","lines":[{"sku":"BOOK","quantity":2},{"sku":"PEN","quantity":1}],"coupon":"WELCOME"}'

# 409: PEN is short, so the BOOK of this order is not taken either
curl -X POST localhost:3000/orders -H 'content-type: application/json' \
  -d '{"customer":"lee","lines":[{"sku":"BOOK","quantity":1},{"sku":"PEN","quantity":5}]}'

# 201 with discount 0: the coupon is already redeemed, the order goes on
curl -X POST localhost:3000/orders -H 'content-type: application/json' \
  -d '{"customer":"park","lines":[{"sku":"BOOK","quantity":1}],"coupon":"WELCOME"}'

curl localhost:3000/orders/1
curl -X POST localhost:3000/orders/1/cancel
```

`audit_log` then holds one `order.requested` row for each of the three
orders, the one that was rolled back included.

## Test

```sh
DEMARCATE_TEST_DATABASE_URL=postgres://postgres:orders@localhost/postgres \
  cargo test -p demarcate-example-axum-orders -- --include-ignored
```

The tests add products, coupons and orders of their own and leave them in
the database.

## In your own project

demarcate is not on crates.io yet. Replace the dependency in `Cargo.toml`
with a release tag of the repository:

```toml
demarcate = { git = "https://github.com/HarryJhin/demarcate", tag = "v0.1.0", features = ["sqlx-postgres"] }
```

When you copy this structure:

- Register the transaction manager once at startup, before the first
  request. The registry is global to the process.
- A `RequiresNew` function takes a second connection while its caller keeps
  the first. Keep the number of concurrent requests at or below half the
  pool size, as `src/main.rs` does. Otherwise a request fails when the pool
  has no connection for it in time.
- Do not write to the database in an `AfterCommit` listener. The
  transaction is over, and the write is not committed. Call a
  `RequiresNew` function when the listener must write.

The example has no authentication and no rate limit. It is a starting
point for the transaction structure, not a complete service.
