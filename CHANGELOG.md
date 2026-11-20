# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All crates of the workspace share one version.

## [Unreleased]

## [0.2.2](https://github.com/HarryJhin/demarcate/compare/v0.2.1...v0.2.2) - 2026-11-20

### Other

- describe the crates.io dependency and build docs.rs with all features ([#11](https://github.com/HarryJhin/demarcate/pull/11))

## [0.2.1](https://github.com/HarryJhin/demarcate/compare/v0.2.0...v0.2.1) - 2026-11-19

### Added

- install the transaction manager in one call ([#9](https://github.com/HarryJhin/demarcate/pull/9))

### Fixed

- build the examples against any version of the workspace ([#7](https://github.com/HarryJhin/demarcate/pull/7))

## [0.2.0](https://github.com/HarryJhin/demarcate/compare/v0.1.0...v0.2.0) - 2026-11-18

### Added

- add drivers for more databases and connection libraries ([#5](https://github.com/HarryJhin/demarcate/pull/5))

### Fixed

- declare the dependency versions that are tested ([#3](https://github.com/HarryJhin/demarcate/pull/3))

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
