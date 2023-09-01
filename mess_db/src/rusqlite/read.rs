use rusqlite::{params, Connection};

use crate::{error::Error, read::ReadMessages, Message};

pub fn get_messages(
    conn: &Connection,
    global_position: i32,
    limit: Option<i32>,
) -> Result<Vec<Message>, Error> {
    let limit = limit.unwrap_or(1_000).clamp(1, 10_000);
    let mut stmt = conn.prepare_cached(
        r#"
        SELECT
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE global_position >= $1
        ORDER BY global_position ASC
        LIMIT $2"#,
    )?;
    let messages = stmt
        .query_and_then(params![global_position, limit], |row| {
            Message::try_from(row)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(messages)
}

pub fn get_stream_messages<'a>(
    conn: &Connection,
    stream_name: &str,
    limit: Option<i32>,
) -> Result<Vec<Message<'a>>, Error> {
    let limit = limit.unwrap_or(1_000).clamp(1, 10_000);
    let mut stmt = conn.prepare_cached(
        r#"
        SELECT
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE stream_name = $1
        ORDER BY global_position ASC
        LIMIT $2"#,
    )?;
    let messages = stmt
        .query_and_then(params![stream_name, limit], |row| {
            Message::try_from(row)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(messages)
}

// ```
// use mess_db::read::ReadMessages;
// use mess_db::sqlite::read::fetch;
// let read_messages = ReadMessages::default()
//     .from_stream("some_stream_name")
//     .with_limit(5);
// fetch(read_messages, &pool).unwrap();
// ```
pub fn fetch<'a>(
    rm: ReadMessages<'_>,
    conn: &'a Connection,
) -> Result<Vec<Message<'a>>, Error> {
    if let Some(stream) = rm.stream_name() {
        get_stream_messages(conn, stream, Some(rm.limit() as i32))
    } else {
        get_messages(conn, rm.global_position() as i32, Some(rm.limit() as i32))
    }
}

pub fn get_latest_stream_message<'a>(
    conn: &Connection,
    stream_name: &str,
) -> Result<Option<Message<'a>>, Error> {
    let mut stmt = conn.prepare_cached(
        r#"
        SELECT 
            global_position,
            position,
            time_ms,
            stream_name,
            message_type,
            data,
            metadata,
            id
        FROM messages
        WHERE stream_name = $1
        ORDER BY position DESC
        LIMIT 1"#,
    )?;
    let msg = stmt
        .query_and_then(params![stream_name], |row| Message::try_from(row))?
        .next()
        .transpose()
        .map_err(|e| e.into());
    msg
}

pub fn get_latest_stream_position(
    conn: &Connection,
    stream_name: &str,
) -> Result<Option<i64>, Error> {
    let mut stmt = conn.prepare_cached(
        r#"
        SELECT position
        FROM messages
        WHERE stream_name = $1
        ORDER BY global_position DESC
        LIMIT 1
        "#,
    )?;
    let msg = stmt
        .query_and_then(params![stream_name], |row| row.get(0))?
        .next()
        .transpose()
        .map_err(|e| e.into());
    msg
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use rusqlite::Connection;

    const ROWS_PER_INSERT: usize = 500;

    fn test_db(rows_per_stream: i64) -> Connection {
        let rows_per_stream = rows_per_stream.max(0) as usize;
        let mut conn = crate::rusqlite::test::new_memory_conn_with_migrations();

        let rows = (0..).map(|i| {
            [
                (format!("{:x<6}.xxxxxx", i), "stream1", i, "X", i, None::<()>),
                (format!("{:x<6}.xxxxxy", i), "stream2", i, "X", i, None::<()>),
            ]
        });

        for i in (0..rows_per_stream).step_by(ROWS_PER_INSERT) {
            let row_count = (rows_per_stream - i).min(ROWS_PER_INSERT);
            let mut sql: String = r#"
            INSERT INTO messages (
                id,
                stream_name,
                position,
                message_type,
                data
            ) VALUES
            "#
            .to_owned();
            let total_row_count =
                rows.clone().skip(i).take(row_count).flatten().count();
            for i in 0..total_row_count {
                sql.push_str("\n(?, ?, ?, ?, ?)");
                if i < (total_row_count - 1) {
                    sql.push_str(",");
                }
            }
            let mut stmt = conn.prepare(&sql).unwrap();
            for (row_i, row) in
                rows.clone().skip(i).take(row_count).flatten().enumerate()
            {
                let j = row_i * 5;
                stmt.raw_bind_parameter(j + 1, row.0)
                    .expect(&format!("bind {}", j + 1));
                stmt.raw_bind_parameter(j + 2, row.1)
                    .expect(&format!("bind {}", j + 2));
                stmt.raw_bind_parameter(j + 3, row.2)
                    .expect(&format!("bind {}", j + 3));
                stmt.raw_bind_parameter(j + 4, row.3)
                    .expect(&format!("bind {}", j + 4));
                stmt.raw_bind_parameter(j + 5, row.4)
                    .expect(&format!("bind {}", j + 5));
            }
            stmt.raw_execute().unwrap();
        }
        conn
    }

    mod fn_get_messages {
        use crate::StreamPos;

        use super::*;
        use pretty_assertions::assert_eq;

        #[rstest]
        fn it_gets_messages_up_to_limit() {
            let conn = test_db(5);
            let messages = get_messages(&conn, 0, Some(5)).unwrap();
            assert_eq!(messages.len(), 5);
            let m = &messages[0];
            assert_eq!(m.global_position, 1);
            assert_eq!(m.stream_position, StreamPos::Serial(0));
            // assert_ne!(m.time_ms, 0);
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, b"0".to_vec());
            assert_eq!(m.metadata, None);
            // assert_eq!(m.id, "0xxxxx.xxxxxx");
        }

        #[rstest]
        fn it_gets_messages_starting_from_given_pos() {
            let conn = test_db(5);
            let messages = get_messages(&conn, 5, Some(2)).unwrap();
            assert_eq!(messages.len(), 2);
            let m = &messages[0];
            assert_eq!(m.global_position, 5);
            assert_eq!(m.stream_position, StreamPos::Serial(2));
            // assert_ne!(m.time_ms, 0);
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, b"2".to_vec());
            assert_eq!(m.metadata, None);
            // assert_eq!(m.id, "2xxxxx.xxxxxx");
        }

        #[rstest]
        fn it_returns_empty_vec_if_pos_too_high() {
            let conn = test_db(5);
            let messages = get_messages(&conn, 500, Some(10)).unwrap();
            assert_eq!(messages.len(), 0);
        }

        #[rstest]
        fn the_lowest_limit_is_1() {
            let conn = test_db(5);
            let messages = get_messages(&conn, 0, Some(-200)).unwrap();
            assert_eq!(messages.len(), 1);
        }

        #[rstest]
        fn the_default_is_1_000() {
            let conn = test_db(550);
            let messages = get_messages(&conn, 0, None).unwrap();
            assert_eq!(messages.len(), 1_000);
        }

        #[rstest]
        fn the_max_is_10_000() {
            let conn = test_db(5_010);
            let messages = get_messages(&conn, 0, Some(100_000)).unwrap();
            assert_eq!(messages.len(), 10_000);
        }
    }

    mod fn_get_stream_messages {
        use super::*;
        use pretty_assertions::assert_eq;
        #[rstest]
        fn it_only_returns_messages_from_given_stream() {
            let conn = test_db(5);
            let messages =
                get_stream_messages(&conn, "stream1", Some(5)).unwrap();
            assert_eq!(messages.len(), 5);
            for message in messages {
                assert_eq!(message.stream_name, "stream1");
            }
        }
    }

    mod fn_get_latest_stream_message {
        use crate::StreamPos;

        use super::*;
        use pretty_assertions::assert_eq;

        #[rstest]
        fn it_returns_messages_with_highest_stream_pos() {
            let conn = test_db(5);
            let m =
                get_latest_stream_message(&conn, "stream1").unwrap().unwrap();
            // assert_ne!(m.time_ms, 0);
            assert_eq!(m.global_position, 9);
            assert_eq!(m.stream_position, StreamPos::Serial(4));
            assert_eq!(m.stream_name, "stream1");
            assert_eq!(m.message_type, "X");
            assert_eq!(m.data, b"4".to_vec());
            assert_eq!(m.metadata, None);
            // assert_eq!(m.id, "4xxxxx.xxxxxx");
        }

        #[rstest]
        fn it_returns_none_if_no_stream() {
            let conn = test_db(5);
            let message =
                get_latest_stream_message(&conn, "no-stream").unwrap();
            assert_eq!(message, None);
        }
    }

    mod fn_get_latest_stream_position {
        use super::*;
        use pretty_assertions::assert_eq;
        #[rstest]
        fn it_returns_last_position_for_stream() {
            let conn = test_db(5);
            let position =
                get_latest_stream_position(&conn, "stream1").unwrap();
            assert_eq!(position, Some(4));
        }

        #[rstest]
        fn it_returns_none_if_no_stream() {
            let conn = test_db(5);
            let position =
                get_latest_stream_position(&conn, "null-stream").unwrap();
            assert_eq!(position, None);
        }
    }
}
