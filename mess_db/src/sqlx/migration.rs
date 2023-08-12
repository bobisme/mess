// future: replace with std::cell:LazyCell
use once_cell::sync::Lazy;
use sqlx::migrate::Migrator;
use sqlx::{SqlitePool, Transaction};
use std::{future::Future, pin::Pin};
// use tracing::{error, info};

use crate::error::{Error, MessResult};

static MIGRATOR: Migrator = sqlx::migrate!();

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type MigrationClosure = Box<
    dyn Send
        + Sync
        + for<'a> Fn(
            &'a mut Transaction<'_, sqlx::Sqlite>,
        ) -> BoxFuture<'a, Result<(), sqlx::Error>>,
>;

pub static MIGRATIONS: Lazy<[MigrationClosure; 0]> = Lazy::new(|| {
    [
        // Migration 1 creates the messages table.
        // Box::new(|tx: &mut Transaction<'_, sqlx::Sqlite>| {
        //     Box::pin(async {
        //         sqlx::query(r#"
        //             CREATE TABLE messages (
        //                 global_position INTEGER PRIMARY KEY AUTOINCREMENT,
        //                 position INTEGER NOT NULL,
        //                 time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
        //                 stream_name TEXT NOT NULL,
        //                 message_type TEXT NOT NULL,
        //                 data TEXT NOT NULL, --JSON
        //                 metadata TEXT, --JSON
        //                 id TEXT NOT NULL UNIQUE
        //             )
        //             STRICT
        //             "#,
        //         // In 0.7, `Transaction` can no longer implement `Executor` directly,
        //         // so it must be dereferenced to the internal connection type.
        //         ).execute(&mut **tx).await?;
        //         Ok(())
        //     })
        // }),
        // Migration 2...
    ]
});

pub async fn mig(pool: &SqlitePool) -> MessResult<()> {
    MIGRATOR.run(pool).await.map_err(|e| Error::external(e.into()))
}
