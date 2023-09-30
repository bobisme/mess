use std::marker::PhantomData;

use super::keys::{GlobalKey, StreamKey, SEPARATOR_CHAR};
use crate::{
    error::{Error, Result},
    read::{GetMessages, OptGlobalPos, OptStream, Unset},
    Message,
};

use super::{
    db::DB,
    record::{GlobalRecord, StreamRecord},
};

pub const LIMIT_MAX: usize = 10_000;
pub const LIMIT_DEFAULT: usize = 1_000;

pub struct MessageIter<'msg, Iter: Iterator<Item = Result<Message<'msg>>>>(
    Iter,
);

pub fn fetch_global<'iter, 'msg, 'db: 'iter>(
    db: &'db DB,
    pos: u64,
    limit: usize,
    // ) -> Result<impl 'iter + Iterator<Item = Result<Message<'msg>>>> {
) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
    let glob_key = pos.to_be_bytes();
    let cf = db.global();
    let iter = db.prefix_iterator_cf(cf, glob_key);
    iter.map(|res| {
        let (k, v) = res.as_ref().map_err(|e| Error::Other(e.to_string()))?;
        let key = GlobalKey::from_bytes(k)?;
        let rec = GlobalRecord::from_bytes(v)?;
        Ok(rec.into_message(key.0))
    })
    .take(limit)
}

pub fn fetch_stream<'iter, 'msg, 'db: 'iter>(
    db: &'db DB,
    stream_name: impl AsRef<str> + 'iter,
    limit: usize,
) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
    let mut search_key = stream_name.as_ref().to_owned();
    search_key.push(SEPARATOR_CHAR);
    let cf = db.stream();
    let iter = db.prefix_iterator_cf(cf, search_key);
    iter.map(|res| {
        let (k, v) = res?;
        let key = StreamKey::from_bytes(k)?;
        let rec = StreamRecord::from_bytes(v)?;
        Ok(rec.into_message(key.stream, key.position))
    })
    .take_while(move |res| match res {
        Ok(msg) => msg.stream_name == stream_name.as_ref(),
        Err(_) => true,
    })
    .take(limit)
}

// pub struct Fetch;
pub struct Fetch<Param> {
    _mark: PhantomData<Param>,
}

impl Fetch<OptGlobalPos> {
    pub fn fetch<'iter, 'msg, 'db: 'iter>(
        db: &'db DB,
        opts: GetMessages<Unset, OptGlobalPos, Unset>,
    ) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
        fetch_global(db, opts.start_global_position.0, opts.limit)
    }
}

impl<'iter, 's: 'iter> Fetch<(OptStream<'s>, OptGlobalPos)> {
    pub fn fetch<'msg, 'db: 'iter>(
        db: &'db DB,
        opts: GetMessages<OptStream<'s>, OptGlobalPos, Unset>,
    ) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
        let stream = opts.stream.to_owned();
        fetch_global(db, opts.start_global_position.0, opts.limit).filter(
            move |res| {
                match res {
                    Ok(rec) => rec.stream_name == stream.0.as_ref(),
                    // pass along all errors regardless of prefix
                    Err(_) => true,
                }
            },
        )
    }
}

impl<'iter, 's: 'iter> Fetch<OptStream<'s>> {
    pub fn fetch<'msg, 'db: 'iter>(
        db: &'db DB,
        opts: GetMessages<OptStream<'s>, Unset, Unset>,
    ) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
        fetch_stream(db, opts.stream.0, opts.limit)
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
        write::WriteMessage,
        StreamPos,
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
            let expected_stream_position = x.map(StreamPos::Sequential);
            let i = match x {
                Some(x) => x + 1,
                None => 0,
            };
            [
                WriteMessage {
                    id: Id::from_str(
                        format!("{:x>6x}-xxxxxxxx-xxxxxx", i).as_str(),
                    )
                    .unwrap(),
                    stream_name: "stream1".into(),
                    message_type: "MessageType".into(),
                    data: data[..].into(),
                    metadata: meta[..].into(),
                    expected_stream_position,
                },
                WriteMessage {
                    id: Id::from_str(
                        format!("{:y>6x}-yyyyyyyy-yyyyyy", i).as_str(),
                    )
                    .unwrap(),
                    stream_name: "stream2".into(),
                    message_type: "MessageType".into(),
                    data: data[..].into(),
                    metadata: [][..].into(),
                    expected_stream_position,
                },
            ]
        });
        rows.take(rows_per_stream).flatten().for_each(|msg| {
            write_mess(&conn, msg, &mut ser).unwrap();
        });

        conn
    }

    mod test_get_messages {
        use crate::StreamPos;

        use super::*;
        use assert2::assert;

        #[rstest]
        fn it_gets_messages_up_to_limit() {
            let db = test_db(5);
            let opts = GetMessages::default().from_global(0).with_limit(6);
            let messages = Fetch::<OptGlobalPos>::fetch(&db, opts);
            let messages: Result<Vec<_>> = messages.collect();
            let messages = messages.unwrap();
            for msg in messages.iter() {
                eprintln!("read msg = {:?}", msg);
            }

            assert!(messages.len() == 6);
            let m = &messages[0];
            assert!(m.global_position == 1);
            assert!(m.stream_position == StreamPos::Sequential(0));
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
            let opts = GetMessages::default().from_global(5);
            let messages = Fetch::<OptGlobalPos>::fetch(&db, opts)
                .collect::<Result<Vec<_>>>()
                .unwrap();
            // assert!(messages.len() == 2);
            let m = &messages[0];
            assert!(m.global_position == 5);
            assert!(m.stream_position == StreamPos::Sequential(2));
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
            let opts = GetMessages::default().from_global(500);
            let iter = Fetch::<OptGlobalPos>::fetch(&db, opts);
            assert!(iter.count() == 0);
        }

        #[rstest]
        fn it_only_returns_messages_from_given_stream() {
            let db = test_db(5);
            let opts = GetMessages::default().in_stream("stream1");
            let messages = Fetch::<OptStream<'_>>::fetch(&db, opts)
                .collect::<Result<Vec<_>>>()
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
