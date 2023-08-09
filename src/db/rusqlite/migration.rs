use crate::error::{Error, MessResult};
use once_cell::sync::Lazy;
use rusqlite::{Connection, Transaction};
use tracing::{error, info};

// select unixepoch('2020-01-01');
const HLC_EPOCH: u64 = 1577836800;
const CLOCK_RESOLUTION_MS: u64 = 50;
const TIME_FACTOR: u64 = 1000 / CLOCK_RESOLUTION_MS;

// struct Migration<'a>(dyn);

type MigrationFn =
    Box<dyn Send + Sync + Fn(&Transaction) -> rusqlite::Result<()>>;

static MIGRATIONS: Lazy<[MigrationFn; 1]> = Lazy::new(|| {
    [
        // Migration 1 creates the messages table.
        Box::new(|tx: &Transaction| {
            tx.execute(
                &format!(
                    r#"
            CREATE TABLE messages (
                global_position INTEGER PRIMARY KEY AUTOINCREMENT,
                position INTEGER NOT NULL,
                time_ms INTEGER NOT NULL DEFAULT (
                CAST(
                    (CASE WHEN sqlite_version() >= '3.42.0' THEN
                    unixepoch('subsec')
                    ELSE
                    unixepoch()
                    END) * 1000 AS integer)
                ),
                stream_name TEXT NOT NULL,
                message_type TEXT NOT NULL,
                data TEXT NOT NULL, --JSON
                metadata TEXT, --JSON
                id TEXT NOT NULL UNIQUE,
                -- ord is the HLC timestamp for when the recording
                -- server recorded the event
                ord INTEGER DEFAULT (-(
                ((
                    CAST((
                    CASE WHEN sqlite_version() >= '3.42.0' THEN
                        unixepoch('subsec') - {HLC_EPOCH}
                    ELSE
                        unixepoch() - {HLC_EPOCH}
                    END
                    ) * {TIME_FACTOR} as integer)
                ) << 16)
                -- + (0 << 56) -- era
                )),

                -- Virtual columns
                ord_unix REAL AS (
                    cast(ord >> 16 as float) / {TIME_FACTOR} + {HLC_EPOCH}
                ) VIRTUAL,
                ord_time TEXT AS (
                    datetime(ord_unix, 'unixepoch', 'subsec')
                ) VIRTUAL,
                category TEXT AS (
                    substring(stream_name, 1, instr(stream_name, '-') - 1)
                ) VIRTUAL,
                stream_id TEXT AS (
                    substring(stream_name, instr(stream_name, '-') + 1)
                ) VIRTUAL,
                cardinal_id TEXT AS (
                    substring(stream_id, 1, instr(stream_id, '+') - 1)
                ) VIRTUAL
            )
            STRICT;
        "#
                ),
                [],
            )?;
            tx.execute("DROP INDEX IF EXISTS messages_ord", [])?;
            tx.execute(
                "CREATE UNIQUE INDEX messages_ord ON messages (ord)",
                [],
            )?;
            tx.execute("DROP TRIGGER IF EXISTS clock_timestamp", [])?;
            tx.execute(
                r#"
CREATE TRIGGER clock_timestamp
AFTER INSERT ON messages
FOR EACH ROW
BEGIN
    UPDATE messages 
    SET ord = MAX(-NEW.ord, (SELECT MAX(ord) + 1 FROM messages))
    WHERE global_position = NEW.global_position AND NEW.ord < 0;
END;
        "#,
                [],
            )?;
            // CHECK: messages.position must match the next sequential
            tx.execute("DROP TRIGGER IF EXISTS check_stream_position", [])?;
            tx.execute(
                r#"
CREATE TRIGGER check_stream_position
BEFORE INSERT ON messages
FOR EACH ROW
BEGIN
    SELECT CASE WHEN 
        IFNULL((
            SELECT position
            FROM messages
            WHERE stream_name = NEW.stream_name
            ORDER BY global_position DESC
            LIMIT 1
        ), -1) != NEW.position - 1 
    THEN RAISE(ROLLBACK, 'stream position mismatch') END;
END;
        "#,
                [],
            )?;
            // INDEX: messages.id
            tx.execute("DROP INDEX IF EXISTS messages_id", [])?;
            tx.execute("CREATE UNIQUE INDEX messages_id ON messages (id)", [])?;
            Ok(())
        }),
        // Migration 2...
        // Box::new(|tx: &Transaction| {
        //     tx.execute("", [])?;
        //     Ok(())
        // }),
    ]
});

/// Gets PRAGMA user_version.
fn get_user_version(conn: &Connection) -> MessResult<i32> {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|err| Error::external(err.into()))
}

/// Sets PRAGMA user_version = `version`.
fn set_user_version(conn: &Connection, version: i32) -> MessResult<()> {
    conn.pragma_update(None, "user_version", version)
        .map_err(|err| Error::external(err.into()))
}

/// Runs thoughs migrations which have not been run and runs them, updating
/// the tracked migration version in the db.
pub fn migrate(conn: &mut Connection) -> MessResult<()> {
    let starting_version = get_user_version(conn)?;

    for (version, migration) in
        MIGRATIONS.iter().enumerate().skip(starting_version as usize)
    {
        let version = version as i32 + 1;
        let tx = conn.transaction()?;
        info!("Starting migration version {}", version);

        if let Err(err) = migration(&tx) {
            let err = Error::MigrationFailed(version, err.into());
            error!(?err, "Migration failed");
            return Err(err);
        }
        set_user_version(&tx, version)?;
        info!("Migration version {} succeeded", version);
        tx.commit()?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use rusqlite::params;

    use crate::error::Error;

    #[fixture]
    fn test_db() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    mod test_user_version {
        use super::*;

        #[rstest]
        fn user_version_is_0_by_default(test_db: Connection) {
            assert_eq!(get_user_version(&test_db).unwrap(), 0);
        }

        #[rstest]
        fn get_user_version_works(test_db: Connection) {
            test_db.pragma_update(None, "user_version", 42).unwrap();
            assert_eq!(get_user_version(&test_db).unwrap(), 42);
        }

        #[rstest]
        fn set_user_version_works(test_db: Connection) {
            assert_eq!(get_user_version(&test_db).unwrap(), 0);
            set_user_version(&test_db, 42).unwrap();
            assert_eq!(get_user_version(&test_db).unwrap(), 42);
        }
    }

    #[rstest]
    fn creates_messages_table(test_db: Connection) {
        let mut conn = test_db;
        migrate(&mut conn).unwrap();

        let rec = conn.query_row(
            r#"SELECT type, name, tbl_name FROM sqlite_schema WHERE tbl_name = ?1"#,
            params!["messages"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        )
        .unwrap();
        assert_eq!(
            rec,
            (
                Some("table".to_owned()),
                Some("messages".to_owned()),
                Some("messages".to_owned())
            )
        );
    }

    #[rstest]
    fn sets_user_version(test_db: Connection) {
        let mut conn = test_db;
        migrate(&mut conn).unwrap();

        let uver: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(uver as usize, MIGRATIONS.len());
    }

    #[rstest]
    // #[should_panic(expected = "table messages already exists")]
    fn fails_if_table_exists() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute(
            r#"
            CREATE TABLE messages (
                global_position INTEGER PRIMARY KEY AUTOINCREMENT
            )
            "#,
            [],
        )
        .unwrap();

        let res = migrate(&mut conn);
        assert!(matches!(res, Err(Error::MigrationFailed(1, _))));

        let uver: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(uver as usize, 0);
    }
}
