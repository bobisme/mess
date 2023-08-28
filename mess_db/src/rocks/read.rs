use std::borrow::Cow;

use crate::{error::MessResult, Message, StreamPos};

use super::{
    db::DB,
    record::{GlobalKey, GlobalRecord, StreamKey, StreamRecord},
};

const MAX_LIMIT: u64 = 10_000;
const DEFAULT_LIMIT: u64 = 1_000;

// type states for GetMessages options
#[derive(Default, Clone, Copy)]
struct OptUnset;
#[derive(Default, Clone)]
struct OptPrefix<'a>(Cow<'a, str>);
#[derive(Default, Clone, Copy)]
struct OptGlobalPos(u64);
#[derive(Clone, Copy)]
struct OptStreamPos(StreamPos);

#[derive(Clone, PartialEq, PartialOrd)]
pub struct GetMessages<P, G, S> {
    start_global_position: G,
    start_stream_position: S,
    limit: u64,
    prefix: P,
}

impl Default for GetMessages<OptUnset, OptUnset, OptUnset> {
    fn default() -> Self {
        Self {
            start_global_position: Default::default(),
            start_stream_position: Default::default(),
            limit: DEFAULT_LIMIT,
            prefix: Default::default(),
        }
    }
}

impl GetMessages<OptUnset, OptUnset, OptUnset> {
    pub fn new() -> Self {
        GetMessages::default()
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = limit.clamp(1, MAX_LIMIT);
        self
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn from_global(
        mut self,
        position: u64,
    ) -> GetMessages<P, OptGlobalPos, S> {
        GetMessages {
            start_global_position: OptGlobalPos(position),
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            prefix: self.prefix,
        }
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn in_stream(mut self, name: &str) -> GetMessages<OptPrefix, G, S> {
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            prefix: OptPrefix(
                format!("{name}{}", super::record::SEPARATOR).into(),
            ),
        }
    }
}

pub fn fetch_global<'a>(
    db: &'a DB,
    pos: u64,
) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
    let glob_key = pos.to_be_bytes();
    let cf = db.global();
    let iter = db.prefix_iterator_cf(cf, glob_key);
    let iter = iter.map(|res| {
        let (k, v) = res?;
        let key = GlobalKey::from_bytes(&k)?;
        let rec = GlobalRecord::from_bytes(&v)?;
        Ok(rec.to_message(key.0))
    });
    return Ok(iter);
}

pub fn fetch_stream<'a, P: AsRef<[u8]>>(
    db: &'a DB,
    start_key: P,
) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
    let cf = db.stream();
    let iter = db.prefix_iterator_cf(cf, start_key);
    let iter = iter.map(|res| {
        let (k, v) = res?;
        let key = StreamKey::from_bytes(&k)?;
        let rec = StreamRecord::from_bytes(&v)?;
        Ok(rec.to_message(key.stream, key.position))
    });
    Ok(iter)
}

impl<'a> GetMessages<OptUnset, OptGlobalPos, OptUnset> {
    pub fn fetch(
        &self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
        fetch_global(db, self.start_global_position.0)
    }
}

impl<'a> GetMessages<OptPrefix<'a>, OptGlobalPos, OptUnset> {
    pub fn fetch(
        &self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
        let prefix = self.prefix.to_owned();
        Ok(fetch_global(db, self.start_global_position.0)?.filter(move |res| {
            match res {
                Ok(rec) => rec.stream_name.starts_with(prefix.0.as_ref()),
                // pass along all errors regardless of prefix
                Err(_) => true,
            }
        }))
    }
}

impl<'a> GetMessages<OptPrefix<'a>, OptUnset, OptUnset> {
    pub fn fetch(
        &'a self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
        fetch_stream(db, self.prefix.0.as_ref())
    }
}

impl<'a> GetMessages<OptPrefix<'a>, OptUnset, OptStreamPos> {
    pub fn fetch(
        &self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
        let key = StreamKey::new(
            self.prefix.0.as_ref(),
            self.start_stream_position.0,
        );
        fetch_stream(db, key.as_bytes())
    }
}

// #[cfg(test)]
// mod test {
//     use std::cell::Cell;
//
//     use super::*;
//     use rstest::*;
//     use tracing::{error, info};
//
//     const ROWS_PER_INSERT: usize = 500;
//
//     fn test_db(rows_per_stream: i64) -> Connection {
//         let rows_per_stream = rows_per_stream.max(0) as usize;
//         let mut conn = crate::rusqlite::test::new_memory_conn_with_migrations();
//
//         let rows = (0..).map(|i| {
//             [
//                 (format!("{:x<6}.xxxxxx", i), "stream1", i, "X", i, None::<()>),
//                 (format!("{:x<6}.xxxxxy", i), "stream2", i, "X", i, None::<()>),
//             ]
//         });
//
//         for i in (0..rows_per_stream).step_by(ROWS_PER_INSERT) {
//             let row_count = (rows_per_stream - i).min(ROWS_PER_INSERT);
//             let mut sql: String = r#"
//             INSERT INTO messages (
//                 id,
//                 stream_name,
//                 position,
//                 message_type,
//                 data
//             ) VALUES
//             "#
//             .to_owned();
//             let total_row_count =
//                 rows.clone().skip(i).take(row_count).flatten().count();
//             for i in 0..total_row_count {
//                 sql.push_str("\n(?, ?, ?, ?, ?)");
//                 if i < (total_row_count - 1) {
//                     sql.push_str(",");
//                 }
//             }
//             let mut stmt = conn.prepare(&sql).unwrap();
//             for (row_i, row) in
//                 rows.clone().skip(i).take(row_count).flatten().enumerate()
//             {
//                 let j = row_i * 5;
//                 stmt.raw_bind_parameter(j + 1, row.0)
//                     .expect(&format!("bind {}", j + 1));
//                 stmt.raw_bind_parameter(j + 2, row.1)
//                     .expect(&format!("bind {}", j + 2));
//                 stmt.raw_bind_parameter(j + 3, row.2)
//                     .expect(&format!("bind {}", j + 3));
//                 stmt.raw_bind_parameter(j + 4, row.3)
//                     .expect(&format!("bind {}", j + 4));
//                 stmt.raw_bind_parameter(j + 5, row.4)
//                     .expect(&format!("bind {}", j + 5));
//             }
//             stmt.raw_execute().unwrap();
//         }
//         conn
//     }
//
//     mod fn_get_messages {
//         use super::*;
//         use assert2::assert;
//
//         #[rstest]
//         fn it_gets_messages_up_to_limit() {
//             let conn = test_db(5);
//             let messages = get_messages(&conn, 0, Some(5)).unwrap();
//             assert!(messages.len() == 5);
//             let m = &messages[0];
//             assert!(m.global_position == 1);
//             assert!(m.position == 0);
//             assert_ne!(m.time_ms, 0);
//             assert!(m.stream_name == "stream1");
//             assert!(m.message_type == "X");
//             assert!(m.data == "0");
//             assert!(m.metadata == None);
//             assert!(m.id == "0xxxxx.xxxxxx");
//         }
//
//         #[rstest]
//         fn it_gets_messages_starting_from_given_pos() {
//             let conn = test_db(5);
//             let messages = get_messages(&conn, 5, Some(2)).unwrap();
//             assert!(messages.len() == 2);
//             let m = &messages[0];
//             assert!(m.global_position == 5);
//             assert!(m.position == 2);
//             assert_ne!(m.time_ms, 0);
//             assert!(m.stream_name == "stream1");
//             assert!(m.message_type == "X");
//             assert!(m.data == "2");
//             assert!(m.metadata == None);
//             assert!(m.id == "2xxxxx.xxxxxx");
//         }
//
//         #[rstest]
//         fn it_returns_empty_vec_if_pos_too_high() {
//             let conn = test_db(5);
//             let messages = get_messages(&conn, 500, Some(10)).unwrap();
//             assert!(messages.len() == 0);
//         }
//
//         #[rstest]
//         fn the_lowest_limit_is_1() {
//             let conn = test_db(5);
//             let messages = get_messages(&conn, 0, Some(-200)).unwrap();
//             assert!(messages.len() == 1);
//         }
//
//         #[rstest]
//         fn the_default_is_1_000() {
//             let conn = test_db(550);
//             let messages = get_messages(&conn, 0, None).unwrap();
//             assert!(messages.len() == 1_000);
//         }
//
//         #[rstest]
//         fn the_max_is_10_000() {
//             let conn = test_db(5_010);
//             let messages = get_messages(&conn, 0, Some(100_000)).unwrap();
//             assert!(messages.len() == 10_000);
//         }
//     }
//
//     mod fn_get_stream_messages {
//         use super::*;
//         use assert2::assert;
//         #[rstest]
//         fn it_only_returns_messages_from_given_stream() {
//             let conn = test_db(5);
//             let messages =
//                 get_stream_messages(&conn, "stream1", Some(5)).unwrap();
//             assert!(messages.len() == 5);
//             for message in messages {
//                 assert!(message.stream_name == "stream1");
//             }
//         }
//     }
//
//     mod fn_get_latest_stream_message {
//         use super::*;
//         use assert2::assert;
//
//         #[rstest]
//         fn it_returns_messages_with_highest_stream_pos() {
//             let conn = test_db(5);
//             let m =
//                 get_latest_stream_message(&conn, "stream1").unwrap().unwrap();
//             assert_ne!(m.time_ms, 0);
//             assert!(m.global_position == 9);
//             assert!(m.position == 4);
//             assert!(m.stream_name == "stream1");
//             assert!(m.message_type == "X");
//             assert!(m.data == "4");
//             assert!(m.metadata == None);
//             assert!(m.id == "4xxxxx.xxxxxx");
//         }
//
//         #[rstest]
//         fn it_returns_none_if_no_stream() {
//             let conn = test_db(5);
//             let message =
//                 get_latest_stream_message(&conn, "no-stream").unwrap();
//             assert!(message == None);
//         }
//     }
//
//     mod fn_get_latest_stream_position {
//         use super::*;
//         use assert2::assert;
//         #[rstest]
//         fn it_returns_last_position_for_stream() {
//             let conn = test_db(5);
//             let position =
//                 get_latest_stream_position(&conn, "stream1").unwrap();
//             assert!(position == Some(4));
//         }
//
//         #[rstest]
//         fn it_returns_none_if_no_stream() {
//             let conn = test_db(5);
//             let position =
//                 get_latest_stream_position(&conn, "null-stream").unwrap();
//             assert!(position == None);
//         }
//     }
// }
