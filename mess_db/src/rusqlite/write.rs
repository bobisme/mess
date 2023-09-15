use ident::Id;
use rusqlite::{params, Connection};
use serde::Serialize;

// const ROWS_PER_BULK_INSERT: usize = 100;

use crate::{
    error::{Error, Result},
    write::WriteMessageOld,
    Position, StreamPos,
};

pub fn write_mess<D: Serialize, M: Serialize>(
    conn: &Connection,
    msg: WriteMessageOld<D, M>,
) -> Result<Position> {
    write_message(
        conn,
        msg.id,
        &msg.stream_name,
        &msg.message_type,
        msg.data,
        msg.metadata,
        msg.expected_stream_position,
    )
}

// pub fn write_mess_bulk<'a, D: Serialize, M: Serialize>(
//     conn: &Connection,
//     msgs: impl Iterator<Item = WriteMessage<'a, D, M>>,
// ) -> MessResult<()> {
//     let mut sql: String = r#"
//     INSERT INTO messages (
//         id,
//         stream_name,
//         position,
//         message_type,
//         data
//     ) VALUES
//     "#
//     .to_owned();
//     loop {
//         let placeholder_iter = msgs.
//         let inner = placeholder_iter.take(ROWS_PER_BULK_INSERT);
//         let mut count = 0;
//         for _ in inner {
//             if count > 1 {
//                 sql.push_str(",");
//             }
//             sql.push_str("\n(?, ?, ?, ?, ?)");
//         }
//     }
//     // for msg in msgs {
//     //     write_message(
//     //         &tx,
//     //         msg.id,
//     //         &msg.stream_name,
//     //         &msg.message_type,
//     //         msg.data,
//     //         msg.metadata,
//     //         msg.expected_stream_position,
//     //     )?;
//     // }
//     Ok(())
// }

pub fn write_message(
    conn: &Connection,
    msg_id: Id,
    stream_name: &str,
    msg_type: &str,
    data: impl Serialize,
    meta: Option<impl Serialize>,
    expected_stream_position: Option<StreamPos>,
) -> Result<Position> {
    let next_position = expected_stream_position
        .map(|x| x.next())
        .unwrap_or(StreamPos::Sequential(0));
    let msg_id_str = msg_id.to_string();
    let data = serde_json::to_string(&data)?;
    let meta = match meta {
        Some(m) => Some(serde_json::to_string(&m)?),
        None => None,
    };

    let mut stmt = conn.prepare_cached(
        r#"
        INSERT INTO messages (
            id,
            stream_name,
            position,
            message_type,
            data,
            metadata
        ) VALUES (?, ?, ?, ?, ?, ?)
        RETURNING global_position, position"#,
    )?;
    let (global_position, position): (i64, i64) = stmt
        .query_row(
            params![
                msg_id_str,
                stream_name,
                next_position,
                msg_type,
                data,
                meta
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|err| match err {
            rusqlite::Error::SqliteFailure(e, Some(ref msg)) => {
                match (e.code, msg.as_str()) {
                    (
                        rusqlite::ErrorCode::ConstraintViolation,
                        "stream position mismatch",
                    ) => Error::WrongStreamPosition {
                        stream: stream_name.into(),
                        expected: expected_stream_position.map(|x| x.encode()),
                        got: None,
                    },
                    _ => err.into(),
                }
            }
            _ => err.into(),
        })?;

    Ok(Position::new(
        global_position as u64,
        StreamPos::Sequential(position.unsigned_abs()),
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

        fn try_from(
            row: &rusqlite::Row,
        ) -> core::result::Result<Self, Self::Error> {
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
        use std::str::FromStr;

        use super::*;
        use assert2::assert;
        use rstest::*;
        use rusqlite::Connection;
        use serde_json::json;

        use crate::error::Error;

        #[fixture]
        fn test_db<'a>() -> Connection {
            let mut conn = Connection::open_in_memory().unwrap();
            crate::rusqlite::migration::migrate(&mut conn).unwrap();
            conn
        }

        #[rstest]
        fn it_writes_messages(// test_db: Connection,
        ) {
            let test_db = test_db();
            let data = json!({ "one": 1, "two": 2 });
            let meta = json!({ "three": 3, "four": 4 });
            let pos = write_message(
                &test_db,
                Id::from_str("fartxx.poopxx").unwrap(),
                "thing-xyz123.twothr",
                "Donked",
                data,
                Some(&meta),
                None,
            )
            .unwrap();
            assert_eq!(
                pos,
                Position { global: 1, stream: StreamPos::Sequential(0) }
            );
            let mut stmt = test_db.prepare(r#"SELECT
                global_position, position, time_ms, stream_name, message_type, data, metadata, id
            FROM messages
            LIMIT 2"#).unwrap();
            let rows: core::result::Result<Vec<MessageRow>, rusqlite::Error> =
                stmt.query_map([], |row| row.try_into()).unwrap().collect();
            let rows = rows.unwrap();
            assert!(rows.len() == 1);
            let row = &rows[0];
            assert!(row.global_position == 1);
            assert!(row.position == 0);
            assert!(row.time_ms != 0);
            assert!(row.stream_name == "thing-xyz123.twothr");
            assert!(row.message_type == "Donked");
            assert!(row.data == json!({"one":1,"two":2}).to_string());
            assert!(
                row.metadata == Some(json!({"three":3,"four":4}).to_string())
            );
            assert!(row.id == "fartxx.poopxx");
        }

        #[rstest]
        fn it_errors_if_stream_version_is_unexpected(test_db: Connection) {
            let res = write_message(
                &test_db,
                Id::from_str("fartxx.poopxx").unwrap(),
                "thing-xyz123.twothr",
                "Donked",
                json!({ "one": 1, "two": 2 }),
                None::<()>,
                Some(StreamPos::Sequential(77)),
            );
            let err = res.unwrap_err();
            let Error::WrongStreamPosition { stream, expected: _, got: _ } =
                err
            else {
                panic!("wrong error");
            };
            assert!(stream == "thing-xyz123.twothr");
        }

        #[rstest]
        fn it_stores_null_when_metadata_is_none(test_db: Connection) {
            write_message(
                &test_db,
                Id::new(),
                "stream1",
                "X",
                "data",
                None::<()>,
                None,
            )
            .unwrap();
            let rec: MessageRow = test_db
                .query_row("SELECT * FROM messages LIMIT 1", [], |r| {
                    r.try_into()
                })
                .unwrap();
            assert!(rec.metadata == None);
        }

        #[rstest]
        fn it_stores_json_metadata_when_some() {
            let test_db: Connection = test_db();
            write_message(
                &test_db,
                Id::new(),
                "stream2",
                "X",
                "data",
                Some(&json!({ "some": "meta" })),
                None,
            )
            .unwrap();
            let rec: MessageRow = test_db
                .query_row("SELECT * FROM messages LIMIT 1", [], |r| {
                    r.try_into()
                })
                .unwrap();
            assert!(
                rec.metadata == Some(json!({ "some": "meta" }).to_string())
            );
        }
    }
}

#[cfg(test)]
mod testprops {
    use super::*;
    use proptest::prelude::*;
    use rstest::*;
    use rusqlite::Connection;

    fn test_db() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::rusqlite::migration::migrate(&mut conn).unwrap();
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
            assert!(pos == Position { global: 1, stream: StreamPos::Sequential(0) });
        }
    }
}
