use std::marker::PhantomData;

use super::keys::{GlobalKey, StreamKey, SEPARATOR_CHAR};
use crate::{
    error::{Error, Result},
    read::{GetMessages, OptGlobalPos, OptStream, OptStreamPos, Unset},
    Message, StreamPos,
};

use super::{
    db::DB,
    record::{GlobalRecord, StreamRecord},
};

// NOTE: `LIMIT_MAX` / `LIMIT_DEFAULT` have a single source of truth in
// `crate::read` (that's what `GetMessages::with_limit` actually clamps
// against). This module intentionally does not shadow them with a local
// copy — tests and callers here should reference `crate::read::LIMIT_MAX`
// directly so they can never drift from the constant that governs
// production behavior.

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

/// Like [`fetch_stream`], but starts the scan at `from` instead of the
/// stream's first position, enabling paged/tail reads.
pub fn fetch_stream_from<'iter, 'msg, 'db: 'iter>(
    db: &'db DB,
    stream_name: impl AsRef<str> + 'iter,
    from: StreamPos,
    limit: usize,
) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
    let mut search_key = stream_name.as_ref().to_owned();
    search_key.push(SEPARATOR_CHAR);
    let mut search_key = search_key.into_bytes();
    search_key.extend_from_slice(&from.encode().to_be_bytes());
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

impl<'iter, 's: 'iter> Fetch<(OptStream<'s>, OptStreamPos)> {
    pub fn fetch<'msg, 'db: 'iter>(
        db: &'db DB,
        opts: GetMessages<OptStream<'s>, Unset, OptStreamPos>,
    ) -> impl 'iter + Iterator<Item = Result<Message<'msg>>> {
        fetch_stream_from(
            db,
            opts.stream.0,
            opts.start_stream_position.0,
            opts.limit,
        )
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
            let expected_version =
                crate::ExpectedVersion::from(x.map(StreamPos::new));
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
                    expected_version,
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
                    expected_version,
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
            assert!(m.stream_position == StreamPos::new(0));
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
            assert!(m.stream_position == StreamPos::new(2));
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

        #[rstest]
        fn it_gets_stream_messages_from_given_stream_pos() {
            let db = test_db(5);
            let opts = GetMessages::default()
                .in_stream("stream1")
                .from_stream_position(StreamPos::new(2));
            let messages =
                Fetch::<(OptStream<'_>, crate::read::OptStreamPos)>::fetch(
                    &db, opts,
                )
                .collect::<Result<Vec<_>>>()
                .unwrap();

            assert!(messages.len() == 3);
            assert!(messages[0].stream_position == StreamPos::new(2));
            assert!(messages[0].stream_name == "stream1");
            assert!(
                messages.last().unwrap().stream_position
                    == StreamPos::new(4)
            );
        }

        #[rstest]
        fn stream_pos_fetch_does_not_leak_other_streams() {
            let db = test_db(5);
            let opts = GetMessages::default()
                .in_stream("stream1")
                .from_stream_position(StreamPos::new(4));
            let messages =
                Fetch::<(OptStream<'_>, crate::read::OptStreamPos)>::fetch(
                    &db, opts,
                )
                .collect::<Result<Vec<_>>>()
                .unwrap();

            // only the last message of stream1; never spills into stream2
            assert!(messages.len() == 1);
            assert!(messages[0].stream_name == "stream1");
            assert!(messages[0].stream_position == StreamPos::new(4));
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

    /// Acceptance coverage for bn-3ps: the backend paged-read path must
    /// recover a stream in full regardless of how it compares to
    /// [`crate::read::LIMIT_MAX`] — the single real constant that
    /// `GetMessages::with_limit` clamps against (see the `NOTE` at the top
    /// of this module: there is deliberately no local shadow of it here),
    /// by looping pages the same way `mess-store`'s `EventStore::load` and
    /// `MockBackend::read_stream` do: keep fetching from
    /// `last_seen.next()` until a page comes back shorter than the
    /// requested page size.
    ///
    /// SCOPE NOTE (open question for the lead, not resolved here): the
    /// bone's stated acceptance criterion is "a 50,000-event stream loads
    /// correctly through `EventStore::load`". As of this commit,
    /// `mess-store` has no dependency on / `Backend` impl for `mess_db` at
    /// all (`crates/mess-store/Cargo.toml` depends only on `mess-core`), so
    /// that AC cannot literally be exercised from a test living in
    /// `mess_db` — this bone's owned scope. The tests below instead drive
    /// `mess_db`'s own paged-read primitives (`Fetch::fetch` +
    /// `GetMessages`) with a hand-rolled loop that mirrors the paging
    /// contract `EventStore::load` will need once that dependency exists.
    /// That is real coverage of the backend paging behavior this bone
    /// owns, but it is not the same thing as the AC as literally worded,
    /// and closing the AC for real requires either wiring mess-store to
    /// mess_db (out of this bone's scope) or rewording the AC. Flagging
    /// this explicitly rather than presenting the AC as closed.
    mod paging {
        use super::*;
        use crate::read::{GetMessages, LIMIT_MAX, OptStreamPos};
        use assert2::assert;
        use ident::Id;

        /// Write `n` sequential events into `stream` on a fresh temp DB.
        fn write_stream(stream: &str, n: usize) -> SelfDestructingDB {
            let db = SelfDestructingDB::new_tmp();
            let mut ser = test_ser();
            let data = [7u8; 32];
            for i in 0..n {
                let expected = if i == 0 {
                    None
                } else {
                    Some(StreamPos::new((i - 1) as u64))
                };
                let msg = WriteMessage {
                    id: Id::new(),
                    stream_name: stream.into(),
                    message_type: "PagingTestEvent".into(),
                    data: data[..].into(),
                    metadata: [][..].into(),
                    expected_version: expected.into(),
                };
                write_mess(&db, msg, &mut ser).unwrap();
            }
            db
        }

        /// Page through `stream` in `page_size` chunks, exactly like a
        /// caller (`EventStore::load`) would: advance the cursor to
        /// `last.next()` and stop once a page returns fewer than
        /// `page_size` records. Asserts no page ever exceeds `page_size`
        /// (the LIMIT_MAX-enforcement contract) along the way.
        fn page_through_stream(
            db: &SelfDestructingDB,
            stream: &str,
            page_size: usize,
        ) -> Vec<crate::OwnedMessage> {
            let mut all = Vec::new();
            let mut next_pos = StreamPos::new(0);
            loop {
                let opts = GetMessages::default()
                    .in_stream(stream)
                    .from_stream_position(next_pos)
                    .with_limit(page_size);
                let page: Vec<_> =
                    Fetch::<(OptStream<'_>, OptStreamPos)>::fetch(db, opts)
                        .collect::<Result<Vec<_>>>()
                        .unwrap()
                        .into_iter()
                        .map(crate::OwnedMessage::from)
                        .collect();
                assert!(page.len() <= page_size);
                let page_len = page.len();
                if let Some(last) = page.last() {
                    next_pos = last.stream_position.next();
                }
                all.extend(page);
                if page_len < page_size {
                    break;
                }
            }
            all
        }

        fn assert_full_recovery(
            messages: &[crate::OwnedMessage],
            stream: &str,
            expected_count: usize,
        ) {
            assert!(messages.len() == expected_count);
            for (i, m) in messages.iter().enumerate() {
                assert!(m.stream_name == stream);
                assert!(m.stream_position == StreamPos::new(i as u64));
            }
            // No duplicates: positions are strictly increasing, so a set of
            // them has the same cardinality as the list.
            let unique: std::collections::HashSet<u64> = messages
                .iter()
                .map(|m| m.stream_position.position())
                .collect();
            assert!(unique.len() == expected_count);
        }

        #[test]
        fn a_50_000_event_stream_loads_correctly_through_paged_reads() {
            let stream = "stream-50k";
            let db = write_stream(stream, 50_000);
            let messages = page_through_stream(&db, stream, LIMIT_MAX);
            assert_full_recovery(&messages, stream, 50_000);
        }

        #[rstest::rstest]
        #[case::one_under_limit_max(LIMIT_MAX - 1)]
        #[case::exactly_limit_max(LIMIT_MAX)]
        #[case::one_over_limit_max(LIMIT_MAX + 1)]
        fn paged_reads_recover_every_event_at_the_limit_max_boundary(
            #[case] count: usize,
        ) {
            let stream = "stream-boundary";
            let db = write_stream(stream, count);
            let messages = page_through_stream(&db, stream, LIMIT_MAX);
            assert_full_recovery(&messages, stream, count);
        }

        /// A single oversized request (limit > LIMIT_MAX) must still only
        /// return one page's worth: `with_limit` clamps, it never silently
        /// truncates a *paged* read to one page — that's the caller's job,
        /// driven by "page came back shorter than requested".
        #[test]
        fn a_single_request_never_returns_more_than_limit_max() {
            let stream = "stream-clamp";
            let db = write_stream(stream, LIMIT_MAX + 500);
            let opts = GetMessages::default()
                .in_stream(stream)
                .from_stream_position(StreamPos::new(0))
                .with_limit(usize::MAX);
            let page: Vec<_> =
                Fetch::<(OptStream<'_>, OptStreamPos)>::fetch(&db, opts)
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
            assert!(page.len() == LIMIT_MAX);
        }
    }
}
