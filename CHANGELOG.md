# Changelog

All notable changes to this crate are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `scripts/verify-published.sh` and a CI job that check every published
  version against its tag.

## [0.1.0]

### Added

- `run_exclusive`: run a job while holding a lease row, renewing it every
  `renew_interval`, and cancel the job if the lease is lost or cannot be
  renewed within one `ttl`.
- An epoch fencing token, increased on every takeover.
- `check_fence` and `write_transaction`: refuse a stale holder's writes
  inside the caller's own transaction.
- `carry_fence` and `spawn_fenced`: keep the fence across `tokio::spawn`.
- `ensure_table` and `Config::create_table_sql` for the lease table.
- Support for `sqlx::PgPool` (feature `postgres`, default) and
  `sqlx::AnyPool` (feature `any`).

[Unreleased]: https://github.com/rainlanguage/pg-fenced-lease/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rainlanguage/pg-fenced-lease/releases/tag/v0.1.0
