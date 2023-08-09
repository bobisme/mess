use ident::Id;
use rusqlite::{params, Connection};
use serde::Serialize;
use serde_json::Value;

use crate::{db::Position, error::MessResult};

pub fn write_message(
    conn: &Connection,
    msg_id: Id,
    stream_name: &str,
    msg_type: &str,
    data: impl Serialize,
    meta: Option<impl Serialize>,
    expected_version: Option<i64>,
) -> MessResult<Position> {
    let next_position = expected_version.unwrap_or(0);
    let msg_id_str = msg_id.to_string();
    let data = Value::String(serde_json::to_string(&data)?);
    let meta = match meta {
        Some(m) => Some(Value::String(serde_json::to_string(&m)?)),
        None => None,
    };

    conn.execute(
        r#"
        INSERT INTO messages (
            id,
            stream_name,
            position,
            message_type,
            data,
            metadata
        ) VALUES (?, ?, ?, ?, ?, ?)
        "#,
        params![msg_id_str, stream_name, next_position, msg_type, data, meta],
    )?;

    let global_position: i64 = conn.last_insert_rowid();
    Ok(Position::new(
        global_position as u64,
        Some(next_position.unsigned_abs()),
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(PartialEq, Eq, Debug)]
    pub struct MessageRow {
        global_position: i64,
        position: i64,
        time_ms: i64,
        stream_name: String,
        message_type: String,
        data: String,
        metadata: Option<String>,
        id: String,
    }

    impl TryFrom<&rusqlite::Row<'_>> for MessageRow {
        type Error = rusqlite::Error;

        fn try_from(row: &rusqlite::Row) -> Result<Self, Self::Error> {
            Ok(Self {
                global_position: row.get(0)?,
                position: row.get(1)?,
                time_ms: row.get(2)?,
                stream_name: row.get(3)?,
                message_type: row.get(4)?,
                data: row.get(5)?,
                metadata: row.get(6)?,
                id: row.get(7)?,
            })
        }
    }

    mod write_message_fn {
        // use crate::db::sqlite::test::new_memory_pool;

        use crate::error::Error;

        use super::*;
        use rstest::*;
        use serde_json::json;

        #[fixture]
        fn test_db() -> Connection {
            let mut conn = Connection::open_in_memory().unwrap();
            crate::db::rusqlite::migration::migrate(&mut conn).unwrap();
            conn
        }

        #[rstest]
        fn it_writes_messages(
            test_db: Connection,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let data = json!({ "one": 1, "two": 2 });
            let meta = json!({ "three": 3, "four": 4 });
            let pos = write_message(
                &test_db,
                Id::from("fartxx.poopxx"),
                "thing-xyz123.twothr",
                "Donked",
                data,
                Some(&meta),
                None,
            )?;
            assert_eq!(pos, Position { global: 1, stream: Some(0) });
            let mut stmt = test_db.prepare(r#"SELECT (
                global_position, position, stream_name, message_type, data, metadata
            ) FROM messages
            LIMIT 2"#).unwrap();
            let rows: Result<Vec<MessageRow>, rusqlite::Error> =
                stmt.query_map([], |row| row.try_into()).unwrap().collect();
            let rows = rows.unwrap();
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.global_position, 1);
            assert_eq!(row.position, 0);
            assert_ne!(row.time_ms, 0);
            assert_eq!(row.stream_name, "thing-xyz123.twothr");
            assert_eq!(row.message_type, "Donked");
            assert_eq!(row.data, json!({"one":1,"two":2}).to_string());
            assert_eq!(
                row.metadata,
                Some(json!({"three":3,"four":4}).to_string())
            );
            assert_eq!(row.id, "fartxx.poopxx");
            Ok(())
        }

        #[rstest]
        fn it_errors_if_stream_version_is_unexpected(
            test_db: Connection,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let res = write_message(
                &test_db,
                Id::from("fartxx.poopxx"),
                "thing-xyz123.twothr",
                "Donked",
                json!({ "one": 1, "two": 2 }),
                None::<()>,
                Some(77),
            );
            let err = res.unwrap_err();
            if let Error::WrongStreamPosition { stream } = err {
                assert_eq!(stream, "thing-xyz123.twothr");
            } else {
                return Err(err.into());
            }
            Ok(())
        }

        #[rstest]
        fn it_stores_null_when_metadata_is_none(
            test_db: Connection,
        ) -> Result<(), Box<dyn std::error::Error>> {
            write_message(
                &test_db,
                Id::new(),
                "stream1",
                "X",
                "data",
                None::<()>,
                None,
            )?;
            let rec: MessageRow = test_db
                .query_row("SELECT * FROM messages LIMIT 1", [], |r| {
                    r.try_into()
                })
                .unwrap();
            assert_eq!(rec.metadata, None);
            Ok(())
        }

        #[rstest]
        fn it_stores_json_metadata_when_some(
            test_db: Connection,
        ) -> Result<(), Box<dyn std::error::Error>> {
            write_message(
                &test_db,
                Id::new(),
                "stream2",
                "X",
                "data",
                Some(&json!({ "some": "meta" })),
                None,
            )?;
            let rec: MessageRow = test_db
                .query_row("SELECT * FROM messages LIMIT 1", [], |r| {
                    r.try_into()
                })
                .unwrap();
            assert_eq!(
                rec.metadata,
                Some(json!({ "some": "meta" }).to_string())
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod testprops {
    use super::*;
    use proptest::prelude::*;
    use rstest::*;

    fn test_db() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::rusqlite::migration::migrate(&mut conn).unwrap();
        conn
    }

    proptest! {
        #[rstest]
        fn write_message_doesnt_crash(
            msg_type in "\\PC*", stream_name in "\\PC*",
            data in "\\PC*", meta in "\\PC*",
        ) {
                let conn = test_db();
                let pos = write_message(
                    &conn,
                    Id::new(),
                    &stream_name,
                    &msg_type,
                    data,
                    Some(&meta),
                    None,
                )
                .unwrap();
                assert_eq!(pos, Position { global: 1, stream: Some(0) });
        }
    }
}
