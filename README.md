# pg-fenced-lease

One owner per background job across instances, for Rust services on
Postgres and sqlx. Built for setups that run two copies at once, such as
Cloud Run with `--min=1 --max=2`, where revisions overlap during a deploy.

## The principle

A lease alone is not enough. A leader can pause (GC, a frozen VM, a slow
network), its lease can expire, a new leader can take over, and then the old
one wakes up and writes. Martin Kleppmann's
[How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
explains why, and the fix: a **fencing token** that grows on every takeover,
checked by the storage on every write. Chubby calls it a sequencer.

Here the storage is the same Postgres database that holds the lease, so the
check runs inside the writer's own transaction:

1. **Lease row**: `name`, `holder`, `epoch`, `expires_at`. Time always comes
   from the database clock. River (`river_leader`), Oban (`oban_peers`) and
   ShedLock (`lock_until`) keep their leases the same way.
2. **Acquire**: take the row if it is missing or expired, and add 1 to
   `epoch`. The epoch is the fencing token.
3. **Renew** every TTL/3, matching holder and epoch. No match means someone
   took over: the job is cancelled at once.
4. **Fence**: inside the write transaction, lock the lease row and check
   holder, epoch and expiry. A stale leader's write fails and can never
   commit.
5. **Release** on a clean finish by expiring the row, so the standby takes
   over at once.

The fence takes `FOR KEY SHARE`, so a leader's own writes never block its
renewals. A takeover needs `FOR UPDATE`, so it waits until the old leader's
open writes finish.

## Use

```toml
[dependencies]
pg-fenced-lease = { git = "https://github.com/rainlanguage/pg-fenced-lease" }
# features = ["any"] for sqlx::AnyPool
```

```rust
const LEASES: pg_fenced_lease::Config = pg_fenced_lease::Config::new("leases");

// Once, or as a migration: LEASES.create_table_sql()
pg_fenced_lease::ensure_table(&pool, &LEASES).await?;

// In the background job supervisor:
pg_fenced_lease::run_exclusive(&pool, &LEASES, "indexer", run_indexer()).await?;

// Everywhere the job writes:
let mut tx = pg_fenced_lease::write_transaction(&pool).await?;
// ... writes ...
tx.commit().await?;
```

Defaults: 30s TTL, 10s renewal, 1s standby retry.

## Things to know

- **Spawned tasks are not fenced.** The fence lives in a tokio `task_local`,
  and `tokio::spawn` drops it. Use `spawn_fenced` or `carry_fence`.
- **Only fenced transactions are safe.** Writes straight on the pool skip
  the check. Run the fence first in the transaction.
- **Writes outside a job are allowed.** HTTP handlers are not fenced, on
  purpose.
- **Takeover after a crash takes up to one TTL.** After a clean finish, or
  when the job future is dropped, it is about immediate.
- **Renewals use your pool.** A pool that stays exhausted for a whole TTL
  costs the job its lease.
- **Never delete lease rows.** Deleting a row restarts its epoch at 1. Do
  not add a unique index on any column but `name`.

## Tests

```bash
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=pg postgres:17
DATABASE_URL=postgres://postgres:pg@localhost:5432/postgres cargo test --all-features
```

They cover two contenders, a clean hand-over, a dropped holder, a holder
that stops renewing (through a TCP proxy that goes silent), a stale write
refused after a takeover, a stolen lease cancelling the job, renewals going
through while the leader has a write open, a takeover waiting for the old
leader's open write, spawned tasks, and the `Any` driver. CI runs them on
Postgres 14 and 17.
