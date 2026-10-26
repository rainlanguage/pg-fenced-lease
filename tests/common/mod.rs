//! Shared helpers for tests against a disposable Postgres in `DATABASE_URL`.
#![allow(dead_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pg_fenced_lease::Config;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

pub fn database_url() -> url::Url {
    let url = std::env::var("DATABASE_URL").expect(
        "set DATABASE_URL to a disposable Postgres, \
         e.g. postgres://postgres:pg@localhost:5432/postgres",
    );
    url.parse().expect("DATABASE_URL is a URL")
}

pub async fn pool_at(url: &url::Url) -> PgPool {
    PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url.as_str())
        .await
        .unwrap()
}

pub async fn pool() -> PgPool {
    pool_at(&database_url()).await
}

/// A lease table no other test uses, so tests can run in parallel.
pub fn table() -> &'static str {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let id = nanos ^ NEXT.fetch_add(0x2545_F491, Ordering::Relaxed);
    Box::leak(format!("leases_test_{id:08x}").into_boxed_str())
}

/// Fast timings in a fresh table, so tests finish in seconds.
pub async fn config(pool: &PgPool) -> Config {
    let config = Config {
        ttl: Duration::from_secs(2),
        renew_interval: Duration::from_millis(500),
        retry_interval: Duration::from_millis(100),
        ..Config::new(table())
    };
    pg_fenced_lease::ensure_table(pool, &config).await.unwrap();
    config
}

/// Timings where only the fence can notice a lost lease.
pub async fn fence_only_config(pool: &PgPool) -> Config {
    Config {
        ttl: Duration::from_secs(600),
        renew_interval: Duration::from_secs(300),
        ..config(pool).await
    }
}

/// Make the lease look expired, from outside the holder.
pub async fn expire(pool: &PgPool, config: &Config, name: &str) {
    sqlx::query(&format!(
        "UPDATE {} SET expires_at = clock_timestamp() - interval '1 second'
         WHERE name = $1",
        config.table
    ))
    .bind(name)
    .execute(pool)
    .await
    .unwrap();
}

/// Take the lease away from its holder, as a new leader would.
pub async fn steal(pool: &PgPool, config: &Config, name: &str) {
    sqlx::query(&format!(
        "UPDATE {} SET holder = 'thief', epoch = epoch + 1 WHERE name = $1",
        config.table
    ))
    .bind(name)
    .execute(pool)
    .await
    .unwrap();
}

pub async fn epoch(pool: &PgPool, config: &Config, name: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT epoch FROM {} WHERE name = $1",
        config.table
    ))
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// A TCP proxy in front of Postgres that can go silent: after `freeze`,
/// it forwards nothing more but keeps both sockets open, like a peer whose
/// machine vanished without a FIN or RST.
pub struct Proxy {
    pub url: url::Url,
    frozen: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    pub async fn start(upstream: &url::Url) -> Self {
        let target = format!(
            "{}:{}",
            upstream.host_str().unwrap(),
            upstream.port().unwrap_or(5432)
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut url = upstream.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        let (frozen, watching) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut parked = tokio::task::JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(&target).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                parked.spawn(pump(client_read, server_write, watching.clone()));
                parked.spawn(pump(server_read, client_write, watching.clone()));
            }
        });
        Self { url, frozen, task }
    }

    pub fn freeze(&self) {
        self.frozen.send_replace(true);
    }
}

async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, mut frozen: watch::Receiver<bool>) {
    let mut buf = vec![0_u8; 8192];
    loop {
        let read = tokio::select! {
            read = from.read(&mut buf) => read,
            _ = frozen.wait_for(|frozen| *frozen) => break,
        };
        let n = match read {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if *frozen.borrow() || to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    if *frozen.borrow() {
        // Hold both halves open so neither peer ever sees a close.
        std::future::pending::<()>().await;
    }
    let _ = to.shutdown().await;
}
