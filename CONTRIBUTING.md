# Contributing

Issues and pull requests are welcome.

## Before you open a pull request

The tests need a disposable Postgres:

```bash
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=pg postgres:17
export DATABASE_URL=postgres://postgres:pg@localhost:5432/postgres
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

A change to how leases are taken, renewed, released or fenced needs a test
that fails without it. Add a line under `[Unreleased]` in `CHANGELOG.md`.

## Releasing

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under the new version,
   and set `version` in `Cargo.toml` to match.
2. Merge that to `main`.
3. Tag the merge commit `vX.Y.Z` and push the tag. The release workflow
   checks that the tag matches `Cargo.toml`, runs the tests, publishes to
   crates.io, and creates the GitHub release from the changelog section.

The first version of a new crate cannot use trusted publishing. For
`0.1.0`, an owner runs `cargo publish` from the tagged commit, then adds a
trusted publisher on crates.io (repository `rainlanguage/pg-fenced-lease`,
workflow `release.yml`, environment `crates-io`). Pushing the tag after
that still creates the GitHub release; the workflow skips a version that
is already on crates.io.
