# quickstart

The smallest program that uses demarcate: one service with a transactional
function, a `RequiresNew` function and a function that joins.

```sh
export DATABASE_URL="postgres://postgres:password@localhost/postgres"
cargo run --manifest-path examples/quickstart/Cargo.toml
```

The program creates the tables `quickstart_orders` and `quickstart_audit` and places one order.

This crate is not a member of the workspace. It builds demarcate the way
an application does, with sqlx features of its own choice, and the CI
builds it for that reason. For a layered application see
`examples/axum-orders`.
