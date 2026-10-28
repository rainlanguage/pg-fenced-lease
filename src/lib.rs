//! One owner per background job across instances: a lease row in Postgres
//! with an epoch fencing token, checked inside every write transaction.
//!
//! When two copies of a service run at once (Cloud Run revisions overlapping
//! during a deploy, or a min-1 max-2 service scaling out), a background job
//! such as a chain indexer or a poller must still run in one place, and a
//! copy that lost the job must not be able to write.
//!
//! This is the fencing-token pattern from Chubby's sequencers and from Martin
//! Kleppmann's [How to do distributed locking][kleppmann], with the lease
//! kept in a database row the way River (`river_leader`), Oban
//! (`oban_peers`) and `ShedLock` (`lock_until`) keep theirs.
//!
//! [kleppmann]: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
//!
//! # How it works
//!
//! 1. **Lease row.** One row per job: `name`, `holder`, `epoch`,
//!    `expires_at`. All time comes from the database clock.
//! 2. **Acquire.** [`run_exclusive`] takes the row if it is missing or
//!    expired, sets itself as holder, and adds 1 to `epoch`. The new epoch is
//!    the fencing token. Other instances retry every
//!    [`Config::retry_interval`].
//! 3. **Renew.** Every [`Config::renew_interval`] the holder pushes
//!    `expires_at` forward, matching on holder and epoch. If no row matches,
//!    someone else took over, and the job is cancelled at once. If renewals
//!    keep failing for a whole [`Config::ttl`], the job is cancelled too.
//! 4. **Fence.** [`write_transaction`] (or [`check_fence`] in your own
//!    transaction) locks the lease row and checks holder, epoch and expiry.
//!    A stale leader gets [`Error::LostLeadership`], and since the check runs
//!    in the same transaction as the write, its write can never commit.
//! 5. **Release.** When the job ends, the holder sets `expires_at` to now, so
//!    a standby takes over at once instead of after the TTL.
//!
//! # Why the row lock is safe and cheap
//!
//! The fence takes `FOR KEY SHARE` on the lease row. A renewal only changes
//! `expires_at`, so it does not conflict with that lock: a leader's own open
//! writes never block its renewals. A takeover locks the row with
//! `FOR UPDATE SKIP LOCKED`, which does conflict, so a standby cannot take
//! over while any fenced write of the old leader is still open. It skips and
//! retries instead of waiting. Several fenced writes of one leader can run at
//! the same time.
//!
//! This relies on `name` being the only unique column. Do not add a unique
//! index on `holder`, `epoch` or `expires_at`.
//!
//! # Warning: the fence does not follow `tokio::spawn`
//!
//! The fence lives in a tokio `task_local`. Code that the job runs directly
//! is fenced. A task the job starts with `tokio::spawn` (or work moved to
//! `spawn_blocking` or a thread) is **not** fenced, and its writes go through
//! even after leadership is lost. Wrap such futures in [`carry_fence`], or
//! use [`spawn_fenced`].
//!
//! Writes made outside any [`run_exclusive`] scope (an HTTP handler, say) are
//! not fenced either. That is on purpose: only the job is exclusive.
//!
//! # Things to know
//!
//! - Only writes made through [`write_transaction`] or after [`check_fence`]
//!   are fenced. Run the fence first in the transaction. Under
//!   `REPEATABLE READ`, a fence run after a renewal committed fails with a
//!   serialisation error instead of passing.
//! - Renewals use the caller's pool. A pool that stays exhausted for a whole
//!   TTL costs the job its lease.
//! - Takeover after a crash takes up to one TTL. After a clean finish it is
//!   immediate. If the [`run_exclusive`] future is dropped, the crate spawns
//!   a best-effort release.
//! - The caller enables a sqlx tokio runtime feature.

mod backend;

use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sqlx::{Pool, Transaction};

pub use backend::Backend;
use backend::Bind;

tokio::task_local! {
    static FENCE: Fence;
}

/// Where the lease rows live and how fast leases turn over. Every instance
/// that shares a job must use the same `table` and `ttl`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// The lease table, optionally schema-qualified. Default `leases`.
    pub table: &'static str,
    /// How long a lease lasts without renewal. Default 30s.
    pub ttl: Duration,
    /// How often the holder renews. Default 10s, a third of the TTL. Must be
    /// at most half the TTL.
    pub renew_interval: Duration,
    /// How long a standby waits between tries. Default 1s.
    pub retry_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self::new("leases")
    }
}

impl Config {
    /// Defaults for `table`: 30s TTL, 10s renewal, 1s retry.
    pub const fn new(table: &'static str) -> Self {
        Self {
            table,
            ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            retry_interval: Duration::from_secs(1),
        }
    }

    /// The statement that creates the lease table. Put it in a migration, or
    /// run it with [`ensure_table`].
    pub fn create_table_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                 name TEXT PRIMARY KEY,
                 holder TEXT NOT NULL,
                 epoch BIGINT NOT NULL,
                 expires_at TIMESTAMPTZ NOT NULL
             )",
            self.table
        )
    }

    fn validate(&self) -> Result<(), Error> {
        let valid_part = |part: &str| {
            let mut chars = part.chars();
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
                && part.len() <= 63
        };
        let parts: Vec<&str> = self.table.split('.').collect();
        if parts.len() > 2 || !parts.iter().all(|part| valid_part(part)) {
            return Err(Error::InvalidConfig(format!(
                "table {:?} is not a plain identifier",
                self.table
            )));
        }
        if self.renew_interval.is_zero()
            || self.retry_interval.is_zero()
            || self.renew_interval > self.ttl / 2
        {
            return Err(Error::InvalidConfig(format!(
                "renew_interval {:?} must be non-zero and at most half of ttl {:?}",
                self.renew_interval, self.ttl
            )));
        }
        if i64::try_from(self.ttl.as_millis()).is_err() {
            return Err(Error::InvalidConfig("ttl is too long".into()));
        }
        Ok(())
    }

    fn ttl_millis(&self) -> i64 {
        i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX)
    }
}

/// Errors from this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A database call failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// Another instance holds the lease now, or this lease expired.
    #[error("background job lost its lease")]
    LostLeadership,
    /// Renewals kept failing for a whole TTL, so the lease may have expired.
    #[error("background job could not renew its lease within the ttl")]
    LeaseExpired,
    /// The [`Config`] cannot work.
    #[error("invalid lease config: {0}")]
    InvalidConfig(String),
}

/// Lets callers that return `sqlx::Error` use `?` on this crate's results.
impl From<Error> for sqlx::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::Database(error) => error,
            other => sqlx::Error::Protocol(other.to_string()),
        }
    }
}

/// The lease a job holds: the table, the job name, this holder's id and the
/// epoch (the fencing token).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fence {
    table: &'static str,
    name: Arc<str>,
    holder: Arc<str>,
    epoch: i64,
}

impl Fence {
    /// The fence of the job this task runs in, if any.
    pub fn current() -> Option<Self> {
        FENCE.try_with(Clone::clone).ok()
    }

    /// The job name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The unique id of this lease holder.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// The fencing token. Every takeover adds 1.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Run `future` with this fence in scope.
    pub async fn scope<F: Future>(self, future: F) -> F::Output {
        FENCE.scope(self, future).await
    }
}

/// Carry the current task's fence (if any) into `future`, so it stays fenced
/// when it runs on another task.
///
/// ```no_run
/// # async fn work() {}
/// tokio::spawn(pg_fenced_lease::carry_fence(work()));
/// ```
pub fn carry_fence<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let fence = Fence::current();
    async move {
        match fence {
            Some(fence) => fence.scope(future).await,
            None => future.await,
        }
    }
}

/// `tokio::spawn` that keeps the current task's fence.
pub fn spawn_fenced<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(carry_fence(future))
}

/// Create the lease table if it is missing.
pub async fn ensure_table<DB: Backend>(pool: &Pool<DB>, config: &Config) -> Result<(), Error> {
    config.validate()?;
    let mut conn = pool.acquire().await?;
    DB::execute(&mut conn, &config.create_table_sql(), &[]).await?;
    Ok(())
}

/// Check, inside the caller's write transaction, that the current job still
/// holds its lease. Does nothing outside a [`run_exclusive`] scope.
///
/// Locks the lease row until the transaction ends, so no takeover can land
/// before this transaction commits. Run it before the transaction's writes.
pub async fn check_fence<DB: Backend>(tx: &mut Transaction<'_, DB>) -> Result<(), Error> {
    let Some(fence) = Fence::current() else {
        return Ok(());
    };
    let sql = format!(
        "SELECT holder = $2 AND epoch = $3 AND expires_at > clock_timestamp()
         FROM {} WHERE name = $1 FOR KEY SHARE",
        fence.table
    );
    let held = DB::fetch_bool(
        &mut **tx,
        &sql,
        &[
            Bind::Text(&fence.name),
            Bind::Text(&fence.holder),
            Bind::Int(fence.epoch),
        ],
    )
    .await?;
    if held == Some(true) {
        Ok(())
    } else {
        Err(Error::LostLeadership)
    }
}

/// Open a write transaction and check the fence first. Every write a job
/// makes should go through this.
pub async fn write_transaction<DB: Backend>(
    pool: &Pool<DB>,
) -> Result<Transaction<'static, DB>, Error> {
    let mut tx = pool.begin().await?;
    check_fence(&mut tx).await?;
    Ok(tx)
}

/// Run `job` once this instance holds the lease `name`.
///
/// Waits, retrying every [`Config::retry_interval`], while another instance
/// holds it. Returns the job's output when it finishes, then releases the
/// lease. Returns an error, after cancelling the job, if another instance
/// took the lease or renewals failed for a whole TTL.
///
/// Writes the job makes through [`write_transaction`] are fenced. See the
/// crate docs for what is not.
pub async fn run_exclusive<DB, F>(
    pool: &Pool<DB>,
    config: &Config,
    name: &str,
    job: F,
) -> Result<F::Output, Error>
where
    DB: Backend,
    F: Future,
{
    config.validate()?;
    let holder: Arc<str> = new_holder().into();
    let (epoch, renewed_at) = loop {
        let started = Instant::now();
        if let Some(epoch) = try_acquire(pool, config, name, &holder).await? {
            break (epoch, started);
        }
        tokio::time::sleep(config.retry_interval).await;
    };
    let fence = Fence {
        table: config.table,
        name: name.into(),
        holder,
        epoch,
    };
    let mut release = Release {
        pool: pool.clone(),
        fence: Some(fence.clone()),
        timeout: config.renew_interval,
    };
    tracing::info!(
        lease = name,
        holder = &*fence.holder,
        epoch,
        "background job acquired its lease"
    );
    let result = fence
        .clone()
        .scope(async {
            tokio::select! {
                output = job => Ok(output),
                error = keep_renewing(pool, config, &fence, renewed_at) => {
                    Err(error)
                }
            }
        })
        .await;
    if let Err(error) = &result {
        tracing::warn!(lease = name, epoch, %error, "background job lost its lease");
    }
    release.now().await;
    result
}

/// A holder id unique to one `run_exclusive` call, readable in the table.
fn new_holder() -> String {
    static CALLS: AtomicU64 = AtomicU64::new(0);
    let label = std::env::var("K_REVISION")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".into());
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(CALLS.fetch_add(1, Ordering::Relaxed));
    format!("{label}/{}/{:016x}", std::process::id(), hasher.finish())
}

/// Take the lease if the row is missing or expired. Returns the new epoch.
async fn try_acquire<DB: Backend>(
    pool: &Pool<DB>,
    config: &Config,
    name: &str,
    holder: &str,
) -> Result<Option<i64>, Error> {
    let table = config.table;
    let ttl = config.ttl_millis();
    let mut tx = pool.begin().await?;
    let inserted = DB::fetch_i64(
        &mut *tx,
        &format!(
            "INSERT INTO {table} (name, holder, epoch, expires_at)
             VALUES ($1, $2, 1,
                     clock_timestamp() + $3::bigint * interval '1 millisecond')
             ON CONFLICT (name) DO NOTHING
             RETURNING epoch"
        ),
        &[Bind::Text(name), Bind::Text(holder), Bind::Int(ttl)],
    )
    .await?;
    if inserted.is_some() {
        tx.commit().await?;
        return Ok(inserted);
    }
    // SKIP LOCKED: a fenced write of the old holder is still open, or another
    // standby is taking over. Either way, try again later.
    let expired = DB::fetch_i64(
        &mut *tx,
        &format!(
            "SELECT epoch FROM {table}
             WHERE name = $1 AND expires_at <= clock_timestamp()
             FOR UPDATE SKIP LOCKED"
        ),
        &[Bind::Text(name)],
    )
    .await?;
    if expired.is_none() {
        return Ok(None);
    }
    let epoch = DB::fetch_i64(
        &mut *tx,
        &format!(
            "UPDATE {table}
             SET holder = $2, epoch = epoch + 1,
                 expires_at =
                     clock_timestamp() + $3::bigint * interval '1 millisecond'
             WHERE name = $1
             RETURNING epoch"
        ),
        &[Bind::Text(name), Bind::Text(holder), Bind::Int(ttl)],
    )
    .await?;
    tx.commit().await?;
    Ok(epoch)
}

async fn keep_renewing<DB: Backend>(
    pool: &Pool<DB>,
    config: &Config,
    fence: &Fence,
    mut renewed_at: Instant,
) -> Error {
    let sql = format!(
        "UPDATE {} SET expires_at =
             clock_timestamp() + $4::bigint * interval '1 millisecond'
         WHERE name = $1 AND holder = $2 AND epoch = $3",
        fence.table
    );
    let ttl = config.ttl_millis();
    loop {
        tokio::time::sleep(config.renew_interval).await;
        let started = Instant::now();
        let renewal = async {
            let mut conn = pool.acquire().await?;
            DB::execute(
                &mut conn,
                &sql,
                &[
                    Bind::Text(&fence.name),
                    Bind::Text(&fence.holder),
                    Bind::Int(fence.epoch),
                    Bind::Int(ttl),
                ],
            )
            .await
        };
        match tokio::time::timeout(config.renew_interval, renewal).await {
            Ok(Ok(0)) => return Error::LostLeadership,
            Ok(Ok(_)) => renewed_at = started,
            Ok(Err(error)) => {
                tracing::warn!(lease = &*fence.name, %error, "lease renewal failed");
            }
            Err(_) => {
                tracing::warn!(lease = &*fence.name, "lease renewal timed out");
            }
        }
        // Measured from before the last good renewal was sent, so this
        // fires no later than the database's own expiry.
        if renewed_at.elapsed() >= config.ttl {
            return Error::LeaseExpired;
        }
    }
}

/// Ends the lease so a standby can take over at once. Runs when the job
/// ends, or as a spawned task if the `run_exclusive` future is dropped.
struct Release<DB: Backend> {
    pool: Pool<DB>,
    fence: Option<Fence>,
    timeout: Duration,
}

impl<DB: Backend> Release<DB> {
    async fn now(&mut self) {
        if let Some(fence) = self.fence.take() {
            release(&self.pool, &fence, self.timeout).await;
        }
    }
}

impl<DB: Backend> Drop for Release<DB> {
    fn drop(&mut self) {
        let Some(fence) = self.fence.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let (pool, timeout) = (self.pool.clone(), self.timeout);
            runtime.spawn(async move { release(&pool, &fence, timeout).await });
        }
    }
}

async fn release<DB: Backend>(pool: &Pool<DB>, fence: &Fence, timeout: Duration) {
    let sql = format!(
        "UPDATE {} SET expires_at = clock_timestamp()
         WHERE name = $1 AND holder = $2 AND epoch = $3",
        fence.table
    );
    let released = async {
        let mut conn = pool.acquire().await?;
        DB::execute(
            &mut conn,
            &sql,
            &[
                Bind::Text(&fence.name),
                Bind::Text(&fence.holder),
                Bind::Int(fence.epoch),
            ],
        )
        .await
    };
    match tokio::time::timeout(timeout, released).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::debug!(lease = &*fence.name, %error, "lease release failed");
        }
        Err(_) => tracing::debug!(lease = &*fence.name, "lease release timed out"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_bad_tables_and_timings() {
        assert!(Config::default().validate().is_ok());
        assert!(Config::new("app.leases").validate().is_ok());
        for table in ["", "1x", "a;drop", "a.b.c", "lea ses"] {
            assert!(Config::new(table).validate().is_err(), "{table}");
        }
        let slow = Config {
            renew_interval: Duration::from_secs(20),
            ..Config::default()
        };
        assert!(slow.validate().is_err());
        let huge = Config {
            renew_interval: Duration::MAX,
            ..Config::default()
        };
        assert!(huge.validate().is_err());
    }

    #[test]
    fn holders_are_unique_per_call() {
        assert_ne!(new_holder(), new_holder());
    }

    #[test]
    fn crate_errors_become_sqlx_errors() {
        let error: sqlx::Error = Error::LostLeadership.into();
        assert!(error.to_string().contains("lost its lease"));
    }

    #[tokio::test]
    async fn carry_fence_keeps_the_scope_across_spawn() {
        let fence = Fence {
            table: "leases",
            name: "job".into(),
            holder: "me".into(),
            epoch: 3,
        };
        let (plain, carried) = fence
            .clone()
            .scope(async {
                let plain = tokio::spawn(async { Fence::current() });
                let carried = spawn_fenced(async { Fence::current() });
                (plain.await.unwrap(), carried.await.unwrap())
            })
            .await;
        assert_eq!(plain, None);
        assert_eq!(carried, Some(fence));
    }
}
