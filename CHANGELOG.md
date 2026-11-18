# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All crates of the workspace share one version.

## [Unreleased]

## [0.1.0] - 2026-11-19

First release.

- `demarcate-core`: transaction definition, seven propagation modes, five
  isolation levels, transaction status with savepoints, call-bound
  synchronization that needs no async runtime, execution listeners, `TransactionTemplate`,
  transaction-bound listeners, and the transaction manager algorithm
  behind the `TransactionDriver` trait.
- `demarcate-interceptor`: transaction attribute, rollback rules by type,
  name and error value, transaction manager registry, rollback events.
- `demarcate-macros`: `#[transactional]` for an `async fn` and for every
  `async fn` of an `impl` block.
- `demarcate-sqlx`: PostgreSQL transaction manager and `TransactionalPool`,
  which implements `sqlx::Executor`.
- `examples/quickstart` and `examples/axum-orders`: a minimal program and an
  order API with axum, sqlx and PostgreSQL.
