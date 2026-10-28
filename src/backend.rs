//! The few raw queries the crate runs, for each sqlx database it supports.
//!
//! sqlx cannot express "a connection that can execute queries" as a bound
//! that callers inherit, so each supported database gets a small sealed impl
//! instead of a generic `where` clause on every function.

use std::future::Future;
use std::pin::Pin;

type BoxFuture<'c, T> = Pin<Box<dyn Future<Output = T> + Send + 'c>>;

/// A query parameter.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum Bind<'a> {
    Int(i64),
    Text(&'a str),
}

mod sealed {
    pub trait Sealed {}
}

/// A sqlx database this crate can drive. Implemented for `sqlx::Postgres`
/// (feature `postgres`) and `sqlx::Any` pointed at Postgres (feature `any`).
pub trait Backend: sqlx::Database + sealed::Sealed {
    /// Run a statement and return the number of rows it touched.
    #[doc(hidden)]
    fn execute<'c>(
        conn: &'c mut Self::Connection,
        sql: &'c str,
        binds: &'c [Bind<'c>],
    ) -> BoxFuture<'c, Result<u64, sqlx::Error>>;

    /// Fetch the first column of the first row as an integer, if any row.
    #[doc(hidden)]
    fn fetch_i64<'c>(
        conn: &'c mut Self::Connection,
        sql: &'c str,
        binds: &'c [Bind<'c>],
    ) -> BoxFuture<'c, Result<Option<i64>, sqlx::Error>>;

    /// Fetch the first column of the first row as a bool, if any row.
    #[doc(hidden)]
    fn fetch_bool<'c>(
        conn: &'c mut Self::Connection,
        sql: &'c str,
        binds: &'c [Bind<'c>],
    ) -> BoxFuture<'c, Result<Option<bool>, sqlx::Error>>;
}

#[allow(unused_macros)]
macro_rules! backend {
    ($db:ty) => {
        impl sealed::Sealed for $db {}

        impl Backend for $db {
            fn execute<'c>(
                conn: &'c mut Self::Connection,
                sql: &'c str,
                binds: &'c [Bind<'c>],
            ) -> BoxFuture<'c, Result<u64, sqlx::Error>> {
                Box::pin(async move {
                    let mut query = sqlx::query::<$db>(sql);
                    for bind in binds {
                        query = match *bind {
                            Bind::Int(value) => query.bind(value),
                            Bind::Text(value) => query.bind(value),
                        };
                    }
                    Ok(query.execute(conn).await?.rows_affected())
                })
            }

            fn fetch_i64<'c>(
                conn: &'c mut Self::Connection,
                sql: &'c str,
                binds: &'c [Bind<'c>],
            ) -> BoxFuture<'c, Result<Option<i64>, sqlx::Error>> {
                Box::pin(async move {
                    let mut query = sqlx::query_scalar::<$db, i64>(sql);
                    for bind in binds {
                        query = match *bind {
                            Bind::Int(value) => query.bind(value),
                            Bind::Text(value) => query.bind(value),
                        };
                    }
                    query.fetch_optional(conn).await
                })
            }

            fn fetch_bool<'c>(
                conn: &'c mut Self::Connection,
                sql: &'c str,
                binds: &'c [Bind<'c>],
            ) -> BoxFuture<'c, Result<Option<bool>, sqlx::Error>> {
                Box::pin(async move {
                    let mut query = sqlx::query_scalar::<$db, bool>(sql);
                    for bind in binds {
                        query = match *bind {
                            Bind::Int(value) => query.bind(value),
                            Bind::Text(value) => query.bind(value),
                        };
                    }
                    query.fetch_optional(conn).await
                })
            }
        }
    };
}

#[cfg(feature = "postgres")]
backend!(sqlx::Postgres);

#[cfg(feature = "any")]
backend!(sqlx::Any);
