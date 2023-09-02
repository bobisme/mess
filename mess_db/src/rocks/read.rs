use std::borrow::Cow;

use super::keys::{GlobalKey, StreamKey, SEPARATOR, SEPARATOR_CHAR};
use crate::{
    error::{Error, MessResult},
    Message, StreamPos,
};

use super::{
    db::DB,
    record::{GlobalRecord, StreamRecord},
};

const LIMIT_MAX: usize = 10_000;
const LIMIT_DEFAULT: usize = 1_000;

// type states for GetMessages options
#[derive(Default, Clone, Copy)]
pub struct Unset;
#[derive(Default, Clone)]
pub struct OptStream<'a>(Cow<'a, str>);
#[derive(Default, Clone, Copy)]
pub struct OptGlobalPos(u64);
#[derive(Clone, Copy)]
pub struct OptStreamPos(StreamPos);

#[derive(Clone, PartialEq, PartialOrd)]
pub struct GetMessages<Strm, G, S> {
    pub(crate) start_global_position: G,
    pub(crate) start_stream_position: S,
    pub(crate) limit: usize,
    pub(crate) stream: Strm,
}

impl Default for GetMessages<Unset, Unset, Unset> {
    fn default() -> Self {
        Self {
            start_global_position: Default::default(),
            start_stream_position: Default::default(),
            limit: LIMIT_DEFAULT,
            stream: Default::default(),
        }
    }
}

impl GetMessages<Unset, Unset, Unset> {
    #[must_use]
    pub const fn new() -> Self {
        GetMessages {
            start_global_position: Unset,
            start_stream_position: Unset,
            limit: LIMIT_DEFAULT,
            stream: Unset,
        }
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = match limit {
            x if x < 1 => 1,
            x if x > LIMIT_MAX => LIMIT_MAX,
            _ => limit,
        };
        self
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_global(self, position: u64) -> GetMessages<P, OptGlobalPos, S> {
        GetMessages {
            start_global_position: OptGlobalPos(position),
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: self.stream,
        }
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn in_stream(self, name: &str) -> GetMessages<OptStream, G, S> {
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: OptStream(format!("{name}{}", SEPARATOR).into()),
        }
    }
}

pub fn fetch_global(
    db: &DB,
    pos: u64,
    limit: usize,
) -> MessResult<impl '_ + Iterator<Item = Result<Message<'_>, Error>>> {
    let glob_key = pos.to_be_bytes();
    let cf = db.global();
    let iter = db.prefix_iterator_cf(cf, glob_key);
    let iter = iter
        .map(|res| {
            let (k, v) =
                res.as_ref().map_err(|e| Error::Other(e.to_string()))?;
            let key = GlobalKey::from_bytes(k)?;
            let rec = GlobalRecord::from_bytes(v)?;
            Ok(rec.into_message(key.0))
        })
        .take(limit);
    Ok(iter)
}

pub fn fetch_stream<'a>(
    db: &'a DB,
    stream_name: impl AsRef<str> + 'a,
    limit: usize,
) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
    let mut search_key = stream_name.as_ref().to_owned();
    search_key.push(SEPARATOR_CHAR);
    let cf = db.stream();
    let iter = db.prefix_iterator_cf(cf, search_key);
    let iter = iter
        .map(|res| {
            let (k, v) = res?;
            let key = StreamKey::from_bytes(k)?;
            let rec = StreamRecord::from_bytes(v)?;
            Ok(rec.into_message(key.stream, key.position))
        })
        .take_while(move |res| match res {
            Ok(msg) => msg.stream_name == stream_name.as_ref(),
            Err(_) => true,
        })
        .take(limit);
    Ok(iter)
}

impl<'a> GetMessages<Unset, OptGlobalPos, Unset> {
    pub fn fetch(
        self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message<'a>>>> {
        fetch_global(db, self.start_global_position.0, self.limit)
    }
}

impl<'a> GetMessages<OptStream<'a>, OptGlobalPos, Unset> {
    pub fn fetch(
        self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message<'a>>>> {
        let stream = self.stream.to_owned();
        Ok(fetch_global(db, self.start_global_position.0, self.limit)?.filter(
            move |res| {
                match res {
                    Ok(rec) => rec.stream_name == stream.0.as_ref(),
                    // pass along all errors regardless of prefix
                    Err(_) => true,
                }
            },
        ))
    }
}

impl<'a> GetMessages<OptStream<'a>, Unset, Unset> {
    pub fn fetch(
        self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message>>> {
        fetch_stream(db, self.stream.0, self.limit)
    }
}

impl<'a> GetMessages<OptStream<'a>, Unset, OptStreamPos> {
    pub fn fetch(
        self,
        db: &'a DB,
    ) -> MessResult<impl 'a + Iterator<Item = MessResult<Message<'a>>>> {
        // let key = StreamKey::new(self.stream.0, self.start_stream_position.0);
        fetch_stream(db, self.stream.0, self.limit)
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::missing_const_for_fn)]

    use std::str::FromStr;

    use super::*;
    use ident::Id;
    use rstest::*;

    use crate::{
        rocks::{
            db::test::SelfDestructingDB,
            write::{write_mess, WriteSerializer},
        },
        write::WriteSerialMessage,
    };

    fn test_ser() -> WriteSerializer {
        WriteSerializer::new()
    }

    fn test_db(rows_per_stream: i64) -> SelfDestructingDB {
        let rows_per_stream = rows_per_stream.max(0) as usize;
        let conn = SelfDestructingDB::new_tmp();
        let mut ser = test_ser();

        let data = [100u8; 100];
        let meta = [99u8; 100];

        let rows = std::iter::once(None).chain((0u64..).map(Some)).map(|x| {
            let expected_position = x.map(StreamPos::Serial);
            let i = match x {
                Some(x) => x + 1,
                None => 0,
            };
            [
                WriteSerialMessage {
                    id: Id::from_str(format!("{:x>6x}.xxxxxx", i).as_str())
                        .unwrap(),
                    stream_name: "stream1".into(),
                    message_type: "MessageType".into(),
                    data: data[..].into(),
                    metadata: meta[..].into(),
                    expected_position,
                },
                WriteSerialMessage {
                    id: Id::from_str(format!("{:y>6x}.yyyyyy", i).as_str())
                        .unwrap(),
                    stream_name: "stream2".into(),
                    message_type: "MessageType".into(),
                    data: data[..].into(),
                    metadata: [][..].into(),
                    expected_position,
                },
            ]
        });
        rows.take(rows_per_stream).flatten().for_each(|msg| {
            write_mess(&conn, msg, &mut ser).unwrap();
        });

        conn
    }

    mod test_get_messages {
        use super::*;
        use assert2::assert;

        #[rstest]
        fn it_gets_messages_up_to_limit() {
            let db = test_db(5);
            let messages =
                GetMessages::new().from_global(0).limit(6).fetch(&db);
            let messages = messages.unwrap();
            let messages: MessResult<Vec<_>> = messages.collect();
            let messages = messages.unwrap();
            for msg in messages.iter() {
                eprintln!("read msg = {:?}", msg);
            }

            assert!(messages.len() == 6);
            let m = &messages[0];
            assert!(m.global_position == 1);
            assert!(m.stream_position == StreamPos::Serial(0));
            // assert_ne!(m.time_ms, 0);
            assert!(m.stream_name == "stream1");
            assert!(m.message_type == "MessageType");
            assert!(m.data.len() == 100 && m.data[0] == 100u8);
            let meta = m.metadata.as_ref().unwrap();
            assert!(meta.len() == 100 && meta[0] == 99u8);
            // assert!(m.id == "0xxxxx.xxxxxx");
        }

        #[rstest]
        fn it_gets_messages_starting_from_given_pos() {
            let db = test_db(5);
            let messages = GetMessages::new()
                .from_global(5)
                .fetch(&db)
                .unwrap()
                .collect::<MessResult<Vec<_>>>()
                .unwrap();
            // assert!(messages.len() == 2);
            let m = &messages[0];
            assert!(m.global_position == 5);
            assert!(m.stream_position == StreamPos::Serial(2));
            // assert_ne!(m.time_ms, 0);
            assert!(m.stream_name == "stream1");
            assert!(m.message_type == "MessageType");
            assert!(m.data.len() == 100 && m.data[0] == 100u8);
            let meta = m.metadata.as_ref().unwrap();
            assert!(meta.len() == 100 && meta[0] == 99u8);
            // assert!(m.id == "2xxxxx.xxxxxx");
        }

        #[rstest]
        fn it_returns_empty_iter_if_pos_too_high() {
            let db = test_db(5);
            let iter = GetMessages::new().from_global(500).fetch(&db).unwrap();
            assert!(iter.count() == 0);
        }

        #[rstest]
        fn it_only_returns_messages_from_given_stream() {
            let db = test_db(5);
            let messages = GetMessages::new()
                .in_stream("stream1")
                .fetch(&db)
                .unwrap()
                .collect::<MessResult<Vec<_>>>()
                .unwrap();
            for message in messages {
                assert!(message.stream_name == "stream1");
            }
        }
        //
        //     #[rstest]
        //     fn the_lowest_limit_is_1() {
        //         let conn = test_db(5);
        //         let messages = get_messages(&conn, 0, Some(-200)).unwrap();
        //         assert!(messages.len() == 1);
        //     }
        //
        //     #[rstest]
        //     fn the_default_is_1_000() {
        //         let conn = test_db(550);
        //         let messages = get_messages(&conn, 0, None).unwrap();
        //         assert!(messages.len() == 1_000);
        //     }
        //
        //     #[rstest]
        //     fn the_max_is_10_000() {
        //         let conn = test_db(5_010);
        //         let messages = get_messages(&conn, 0, Some(100_000)).unwrap();
        //         assert!(messages.len() == 10_000);
        //     }
        // }
        //
        // mod fn_get_stream_messages {
        //     use super::*;
        //     use assert2::assert;
        // }
        //
        // mod fn_get_latest_stream_message {
        //     use super::*;
        //     use assert2::assert;
        //
        //     #[rstest]
        //     fn it_returns_messages_with_highest_stream_pos() {
        //         let conn = test_db(5);
        //         let m =
        //             get_latest_stream_message(&conn, "stream1").unwrap().unwrap();
        //         assert_ne!(m.time_ms, 0);
        //         assert!(m.global_position == 9);
        //         assert!(m.position == 4);
        //         assert!(m.stream_name == "stream1");
        //         assert!(m.message_type == "X");
        //         assert!(m.data == "4");
        //         assert!(m.metadata == None);
        //         assert!(m.id == "4xxxxx.xxxxxx");
        //     }
        //
        //     #[rstest]
        //     fn it_returns_none_if_no_stream() {
        //         let conn = test_db(5);
        //         let message =
        //             get_latest_stream_message(&conn, "no-stream").unwrap();
        //         assert!(message == None);
        //     }
        // }
        //
        // mod fn_get_latest_stream_position {
        //     use super::*;
        //     use assert2::assert;
        //     #[rstest]
        //     fn it_returns_last_position_for_stream() {
        //         let conn = test_db(5);
        //         let position =
        //             get_latest_stream_position(&conn, "stream1").unwrap();
        //         assert!(position == Some(4));
        //     }
        //
        //     #[rstest]
        //     fn it_returns_none_if_no_stream() {
        //         let conn = test_db(5);
        //         let position =
        //             get_latest_stream_position(&conn, "null-stream").unwrap();
        //         assert!(position == None);
        //     }
    }
}
