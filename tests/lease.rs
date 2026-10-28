//! Run against a disposable Postgres: `DATABASE_URL=postgres://... cargo test`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Proxy, config, database_url, epoch, expire, fence_only_config, pool, pool_at, steal};
use pg_fenced_lease::{Config, Error, Fence, run_exclusive, spawn_fenced, write_transaction};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

const SECOND: Duration = Duration::from_secs(1);

/// Start a contender that reports `(id, epoch)` once it holds the lease,
/// then runs until the test drops it.
fn contend(
    pool: &sqlx::PgPool,
    config: Config,
    id: u8,
    entered: mpsc::UnboundedSender<(u8, i64)>,
) -> tokio::task::JoinHandle<Result<(), Error>> {
    let pool = pool.clone();
    tokio::spawn(async move {
        run_exclusive(&pool, &config, "job", async {
            entered
                .send((id, Fence::current().unwrap().epoch()))
                .unwrap();
            std::future::pending::<()>().await;
        })
        .await
    })
}

/// Sets a flag when dropped, to show a job was cancelled.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn two_contenders_have_one_owner_and_a_clean_finish_hands_over_at_once() {
    let pool = pool().await;
    let config = config(&pool).await;
    let (finish, finished) = oneshot::channel::<()>();
    let (entered, mut entries) = mpsc::unbounded_channel();
    let first_entered = entered.clone();
    let first_pool = pool.clone();
    let first = tokio::spawn(async move {
        run_exclusive(&first_pool, &config, "job", async {
            first_entered
                .send((1, Fence::current().unwrap().epoch()))
                .unwrap();
            finished.await.unwrap();
        })
        .await
    });
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((1, 1))
    );
    let second = contend(&pool, config, 2, entered);
    assert!(timeout(SECOND, entries.recv()).await.is_err());

    finish.send(()).unwrap();
    first.await.unwrap().unwrap();
    let handed_over = Instant::now();
    // The takeover adds 1 to the epoch.
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((2, 2))
    );
    // Released, not expired: well under the 2s TTL.
    assert!(
        handed_over.elapsed() < SECOND,
        "{:?}",
        handed_over.elapsed()
    );
    second.abort();
}

#[tokio::test]
async fn a_dropped_holder_releases_its_lease() {
    let pool = pool().await;
    let config = config(&pool).await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let first = contend(&pool, config, 1, entered.clone());
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((1, 1))
    );
    let second = contend(&pool, config, 2, entered);
    first.abort();
    let dropped = Instant::now();
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((2, 2))
    );
    assert!(dropped.elapsed() < SECOND, "{:?}", dropped.elapsed());
    second.abort();
}

#[tokio::test]
async fn a_holder_that_stops_renewing_is_replaced_after_the_ttl() {
    let direct = database_url();
    let proxy = Proxy::start(&direct).await;
    let holder_pool = pool_at(&proxy.url).await;
    let standby_pool = pool_at(&direct).await;
    let config = config(&standby_pool).await;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let holder = contend(&holder_pool, config, 1, entered.clone());
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((1, 1))
    );
    let standby = contend(&standby_pool, config, 2, entered);
    assert!(timeout(SECOND, entries.recv()).await.is_err());

    // The holder's network goes silent: no FIN, no RST, no renewals.
    proxy.freeze();
    let frozen = Instant::now();
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((2, 2))
    );
    // 2s TTL, plus one retry and some slack.
    assert!(frozen.elapsed() < 4 * SECOND, "{:?}", frozen.elapsed());
    // The holder gives up on its own once a TTL passes without a renewal.
    let lost = timeout(5 * SECOND, holder).await.unwrap().unwrap();
    assert!(matches!(lost, Err(Error::LeaseExpired)), "{lost:?}");
    standby.abort();
}

#[tokio::test]
async fn a_stale_leader_cannot_write_after_a_takeover() {
    let pool = pool().await;
    let config = fence_only_config(&pool).await;
    let outcome = run_exclusive(&pool, &config, "job", async {
        write_transaction(&pool)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        // The lease expires (say this instance was paused) and a standby
        // takes over. This holder renews too rarely to notice.
        expire(&pool, &config, "job").await;
        let (entered, mut entries) = mpsc::unbounded_channel();
        let standby = contend(&pool, config, 2, entered);
        assert_eq!(
            timeout(5 * SECOND, entries.recv()).await.unwrap(),
            Some((2, 2))
        );
        let stale = write_transaction(&pool).await.map(drop);
        standby.abort();
        stale
    })
    .await;
    assert!(
        matches!(outcome, Ok(Err(Error::LostLeadership))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_renewal_that_finds_the_lease_taken_cancels_the_job() {
    let pool = pool().await;
    let config = config(&pool).await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let (started, is_started) = oneshot::channel();
    let job_pool = pool.clone();
    let job_cancelled = Arc::clone(&cancelled);
    let holder = tokio::spawn(async move {
        run_exclusive(&job_pool, &config, "job", async move {
            let _guard = Dropped(job_cancelled);
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        })
        .await
    });
    is_started.await.unwrap();
    steal(&pool, &config, "job").await;
    // The next renewal (every 500ms) matches no row.
    let lost = timeout(2 * SECOND, holder).await.unwrap().unwrap();
    assert!(matches!(lost, Err(Error::LostLeadership)), "{lost:?}");
    assert!(cancelled.load(Ordering::SeqCst));
    // The release did not undo the theft.
    assert_eq!(epoch(&pool, &config, "job").await, 2);
}

#[tokio::test]
async fn a_leaders_open_write_does_not_block_its_renewals() {
    let pool = pool().await;
    let config = config(&pool).await;
    let expiry = format!(
        "SELECT expires_at::text FROM {} WHERE name = 'job'",
        config.table
    );
    let renewed = run_exclusive(&pool, &config, "job", async {
        let tx = write_transaction(&pool).await.unwrap();
        let before: String = sqlx::query_scalar(&expiry).fetch_one(&pool).await.unwrap();
        // Three renewal intervals with the fence's row lock held.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let after: String = sqlx::query_scalar(&expiry).fetch_one(&pool).await.unwrap();
        tx.commit().await.unwrap();
        before != after
    })
    .await
    .unwrap();
    assert!(renewed);
}

#[tokio::test]
async fn a_takeover_waits_for_the_old_leaders_open_write() {
    let pool = pool().await;
    let config = fence_only_config(&pool).await;
    let (opened, is_open) = oneshot::channel();
    let (finish, finished) = oneshot::channel::<()>();
    let leader_pool = pool.clone();
    let leader = tokio::spawn(async move {
        run_exclusive(&leader_pool, &config, "job", async {
            let tx = write_transaction(&leader_pool).await.unwrap();
            opened.send(()).unwrap();
            finished.await.unwrap();
            tx.commit().await.unwrap();
        })
        .await
    });
    is_open.await.unwrap();
    // Expiring the row is not blocked by the fence's lock...
    timeout(SECOND, expire(&pool, &config, "job"))
        .await
        .unwrap();
    // ...but taking it over is, while the old leader's write is open.
    let (entered, mut entries) = mpsc::unbounded_channel();
    let standby = contend(&pool, config, 2, entered);
    assert!(timeout(SECOND, entries.recv()).await.is_err());

    finish.send(()).unwrap();
    assert_eq!(
        timeout(5 * SECOND, entries.recv()).await.unwrap(),
        Some((2, 2))
    );
    leader.await.unwrap().unwrap();
    standby.abort();
}

#[tokio::test]
async fn spawned_tasks_are_fenced_only_with_spawn_fenced() {
    let pool = pool().await;
    let config = fence_only_config(&pool).await;
    let (plain, carried) = run_exclusive(&pool, &config, "job", async {
        let fenced = {
            let pool = pool.clone();
            spawn_fenced(async move { write_transaction(&pool).await })
        };
        fenced.await.unwrap().unwrap().commit().await.unwrap();

        steal(&pool, &config, "job").await;
        let plain = {
            let pool = pool.clone();
            tokio::spawn(async move { write_transaction(&pool).await.map(drop) })
        };
        let carried = {
            let pool = pool.clone();
            spawn_fenced(async move { write_transaction(&pool).await.map(drop) })
        };
        (plain.await.unwrap(), carried.await.unwrap())
    })
    .await
    .unwrap();
    // The plain spawn lost the fence and wrote as a stale leader.
    assert!(plain.is_ok(), "{plain:?}");
    assert!(matches!(carried, Err(Error::LostLeadership)), "{carried:?}");
}

#[tokio::test]
async fn the_any_driver_is_fenced_too() {
    let direct = pool().await;
    let config = fence_only_config(&direct).await;
    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(database_url().as_str())
        .await
        .unwrap();
    let outcome = run_exclusive(&pool, &config, "job", async {
        write_transaction(&pool)
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        steal(&direct, &config, "job").await;
        write_transaction(&pool).await.map(drop)
    })
    .await;
    assert!(
        matches!(outcome, Ok(Err(Error::LostLeadership))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn writes_outside_a_job_are_not_fenced() {
    let pool = pool().await;
    write_transaction(&pool)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
}
