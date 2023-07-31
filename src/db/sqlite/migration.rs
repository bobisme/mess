// future: replace with std::cell:LazyCell
use once_cell::sync::Lazy;
use sqlx::migrate::Migrator;
use sqlx::{Connection, SqliteConnection, SqlitePool, Transaction};
use std::{future::Future, pin::Pin};
use tracing::{error, info};

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

static MIGRATIONS: Lazy<[MigrationClosure; 1]> = Lazy::new(|| {
    [
        // Migration 1 creates the messages table.
        Box::new(|tx: &mut Transaction<'_, sqlx::Sqlite>| {
            Box::pin(async {
                sqlx::query(r#"
                    CREATE TABLE messages (
                        global_position INTEGER PRIMARY KEY AUTOINCREMENT,
                        position INTEGER NOT NULL,
                        time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
                        stream_name TEXT NOT NULL,
                        message_type TEXT NOT NULL,
                        data TEXT NOT NULL, --JSON
                        metadata TEXT, --JSON
                        id TEXT NOT NULL UNIQUE
                    )
                    STRICT
                    "#,
                // In 0.7, `Transaction` can no longer implement `Executor` directly,
                // so it must be dereferenced to the internal connection type.
                ).execute(&mut **tx).await?;
                Ok(())
            })
        }),
        // Migration 2...
    ]
});

/// Gets PRAGMA user_version.
async fn get_user_version(conn: &mut SqliteConnection) -> MessResult<i32> {
    let rec = sqlx::query!("PRAGMA user_version").fetch_one(&mut *conn).await?;
    Ok(rec.user_version.unwrap_or(0))
}

/// Sets PRAGMA user_version = `version`.
async fn set_user_version(
    conn: &mut SqliteConnection,
    version: i32,
) -> MessResult<()> {
    sqlx::query(&format!("PRAGMA user_version = {};", version))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn mig(pool: &SqlitePool) -> MessResult<()> {
    MIGRATOR.run(pool).await.map_err(|e| Error::external(e.into()))
}

/// Runs thoughs migrations which have not been run and runs them, updating
/// the tracked migration version in the db.
pub async fn migrate(conn: &mut SqliteConnection) -> MessResult<()> {
    let starting_version = get_user_version(conn).await?;

    for (version, migration) in
        MIGRATIONS.iter().enumerate().skip(starting_version as usize)
    {
        let version = version as i32 + 1;
        let mut tx = conn.begin().await?;
        info!("Starting migration version {}", version);

        let res = migration(&mut tx).await;
        if let Err(err) = res {
            let err = Error::MigrationFailed(version, err);
            error!(?err, "Migration failed");
            return Err(err);
        }
        set_user_version(&mut tx, version).await.unwrap();
        info!("Migration version {} succeeded", version);
        tx.commit().await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use sqlx::{Connection, SqliteConnection};

    use crate::error::Error;

    #[fixture]
    async fn test_db() -> SqliteConnection {
        Connection::connect("sqlite::memory:").await.unwrap()
    }

    mod test_user_version {
        use super::*;

        #[rstest]
        async fn user_version_is_0_by_default(
            #[future] test_db: SqliteConnection,
        ) {
            let mut conn = test_db.await;
            assert_eq!(get_user_version(&mut conn).await.unwrap(), 0);
        }

        #[rstest]
        async fn get_user_version_works(#[future] test_db: SqliteConnection) {
            let mut conn = test_db.await;
            sqlx::query(&format!("PRAGMA user_version = {};", 42))
                .execute(&mut conn)
                .await
                .unwrap();
            assert_eq!(get_user_version(&mut conn).await.unwrap(), 42);
        }

        #[rstest]
        async fn set_user_version_works(#[future] test_db: SqliteConnection) {
            let mut conn = test_db.await;
            assert_eq!(get_user_version(&mut conn).await.unwrap(), 0);
            set_user_version(&mut conn, 42).await.unwrap();
            assert_eq!(get_user_version(&mut conn).await.unwrap(), 42);
        }
    }

    #[rstest]
    async fn creates_messages_table(#[future] test_db: SqliteConnection) {
        let mut conn = test_db.await;
        migrate(&mut conn).await.unwrap();

        let rec = sqlx::query!(
            r#" SELECT * FROM sqlite_schema WHERE tbl_name = $1; "#,
            "messages"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            (rec.r#type, rec.name, rec.tbl_name),
            (
                Some("table".to_owned()),
                Some("messages".to_owned()),
                Some("messages".to_owned())
            )
        );
    }

    #[rstest]
    async fn sets_user_version(#[future] test_db: SqliteConnection) {
        let mut conn = test_db.await;
        migrate(&mut conn).await.unwrap();

        let rec = sqlx::query!("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(rec.user_version.unwrap() as usize, MIGRATIONS.len());
    }

    #[rstest]
    // #[should_panic(expected = "table messages already exists")]
    async fn fails_if_table_exists() {
        let mut conn: SqliteConnection =
            Connection::connect("sqlite::memory:").await.unwrap();
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS messages (
                global_position INTEGER PRIMARY KEY AUTOINCREMENT,
                position INTEGER NOT NULL,
                time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'localtime')),
                stream_name TEXT NOT NULL,
                message_type TEXT NOT NULL,
                data TEXT, --JSON
                metadata TEXT, --JSON
                id TEXT UNIQUE
            )
            STRICT
            "#,
        ).execute(&mut conn).await.unwrap();

        let res = migrate(&mut conn).await;
        assert!(matches!(res, Err(Error::MigrationFailed(1, _))));

        let rec = sqlx::query!("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(rec.user_version.unwrap() as usize, 0);
    }
}
