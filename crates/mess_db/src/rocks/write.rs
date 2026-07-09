use std::sync::{atomic::Ordering, Arc};

use super::{
    db::DB,
    keys::{GlobalKey, StreamKey},
    record::{GlobalRecord, StreamRecord},
};
use crate::{
    error::{Error, Result},
    write::{WriteMessage, WriteMessages},
    ExpectedVersion, Position, StreamPos,
};
use rocksdb::{IteratorMode, ReadOptions};

pub fn get_last_global_position(db: &DB) -> Result<GlobalKey> {
    let cached = db.cached_global.load(Ordering::Acquire);
    if cached != 0 {
        return Ok(GlobalKey(cached));
    }
    let mut opts = ReadOptions::default();
    opts.set_async_io(true);
    opts.set_pin_data(true);
    let last = db.iterator_cf_opt(db.global(), opts, IteratorMode::End).next();
    if last.is_none() {
        return Ok(GlobalKey::new(0));
    }
    let result = last
        .unwrap()
        .map(|(key, _)| GlobalKey::from_bytes(&key))
        .map_err(|e| Error::ReadError(e.to_string()))?
        .map_err(|e| Error::ReadError(e.to_string()));
    if let Ok(key) = result.as_ref() {
        db.cached_global.fetch_max(key.0, Ordering::AcqRel);
    }
    result
}

pub fn get_last_stream_position<'a>(
    db: &DB,
    stream: &str,
) -> Result<Option<StreamKey<'a>>> {
    // Instrumentation: this is the disk head read the write path must AVOID in
    // `ExpectedVersion::Any` mode (dx_api friction #3). Counted so tests can
    // assert zero of these for an Any append.
    db.stream_head_reads.fetch_add(1, Ordering::AcqRel);
    let mut opts = ReadOptions::default();
    opts.set_async_io(true);
    opts.set_pin_data(true);
    let last = db
        .iterator_cf_opt(
            db.stream(),
            opts,
            IteratorMode::From(
                &StreamKey::max(stream.into()).as_bytes(),
                rocksdb::Direction::Reverse,
            ),
        )
        .next()
        .transpose()?;
    let Some((key, _)) = last else {
        return Ok(None);
    };
    StreamKey::from_bytes(&key).map(|x| {
        if x.stream == stream {
            Some(x)
        } else {
            None
        }
    })
}

/// Resolve the FIRST stream position an append should write at, enforcing the
/// expected-version precondition.
///
/// - [`ExpectedVersion::NoStream`]: the stream must be empty; first write is at
///   position 0. Reads the disk head to validate.
/// - [`ExpectedVersion::Exact`]: the disk head must equal the expected
///   position; first write is at `head + 1`. Reads the disk head to validate.
/// - [`ExpectedVersion::Any`]: no precondition and NO disk head read — the
///   next position comes from the authoritative in-memory cache
///   ([`DB::cached_stream_head`]), or 0 if the stream is unknown to this
///   process.
fn resolve_first_stream_pos(
    db: &DB,
    expected: ExpectedVersion,
    stream_name: &str,
) -> Result<StreamPos> {
    match expected {
        ExpectedVersion::Any => Ok(db
            .cached_stream_head(stream_name)
            .map_or(StreamPos::new(0), |h| StreamPos::new(h).next())),
        ExpectedVersion::NoStream => {
            match get_last_stream_position(db, stream_name)? {
                None => Ok(StreamPos::new(0)),
                Some(key) => Err(Error::WrongStreamPosition {
                    stream: stream_name.to_string(),
                    expected: None,
                    got: Some(key.position.position()),
                }),
            }
        }
        ExpectedVersion::Exact(v) => {
            match get_last_stream_position(db, stream_name)? {
                Some(key) if key.position == v => Ok(key.position.next()),
                other => Err(Error::WrongStreamPosition {
                    stream: stream_name.to_string(),
                    expected: Some(v.position()),
                    got: other.map(|k| k.position.position()),
                }),
            }
        }
    }
}

/// Reusable serialization buffers. `S` is the initial buffer size; buffers
/// grow to fit the largest record seen, so payloads are not size-capped.
pub struct WriteSerializer<const S: usize = 1024> {
    global_buffer: Vec<u8>,
    stream_buffer: Vec<u8>,
}

impl<const S: usize> WriteSerializer<S> {
    #[must_use]
    pub const fn new() -> Self {
        Self { global_buffer: Vec::new(), stream_buffer: Vec::new() }
    }

    pub fn serialize_global(&mut self, global: &GlobalRecord) -> Result<&[u8]> {
        Self::serialize_into(&mut self.global_buffer, global, "global")
    }

    pub fn serialize_stream(&mut self, stream: &StreamRecord) -> Result<&[u8]> {
        Self::serialize_into(&mut self.stream_buffer, stream, "stream")
    }

    fn serialize_into<'a, T: serde::Serialize>(
        buf: &'a mut Vec<u8>,
        value: &T,
        what: &'static str,
    ) -> Result<&'a [u8]> {
        if buf.is_empty() {
            buf.resize(S.max(64), 0);
        }
        let len = loop {
            match postcard::to_slice(value, buf.as_mut_slice()) {
                Ok(used) => break used.len(),
                Err(postcard::Error::SerializeBufferFull) => {
                    let grown = buf.len().max(64).saturating_mul(2);
                    buf.resize(grown, 0);
                }
                Err(e) => {
                    return Err(Error::SerError(format!("{what}: {e}")))
                }
            }
        };
        Ok(&buf[..len])
    }
}

impl<const S: usize> Default for WriteSerializer<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a whole batch of events as ONE atomic `rocksdb::WriteBatch`.
///
/// `base_global` is the last-assigned global position (event `i` gets
/// `base_global + 1 + i`). `first_stream_pos` is the position of the batch's
/// first event (subsequent events are contiguous). Because every record is put
/// into a single `WriteBatch` and committed with one `db.write`, either all N
/// events become visible or none do — no torn append is possible, and no
/// concurrent writer can interleave records inside the batch.
fn write_batch_records(
    db: &DB,
    stream_name: &str,
    base_global: u64,
    first_stream_pos: StreamPos,
    events: &[crate::write::WriteEvent<'_>],
    ser: &mut WriteSerializer,
) -> Result<Position> {
    if events.is_empty() {
        // Degenerate no-op append: nothing to write. Report the current head.
        let stream = first_stream_pos;
        return Ok(Position { global: base_global, stream });
    }

    let mut batch = rocksdb::WriteBatch::default();
    let mut global = base_global;
    let mut stream_pos = first_stream_pos;
    let mut last_stream_pos = first_stream_pos;
    for event in events {
        global += 1;
        let global_key = GlobalKey::new(global);
        let stream_key = StreamKey::new(stream_name.into(), stream_pos);

        let global_record = GlobalRecord::build(
            &event.id,
            stream_name,
            stream_pos.encode(),
            event.message_type.as_ref(),
            event.data.as_ref(),
            event.metadata.as_ref(),
        );
        batch.put_cf(
            db.global(),
            global_key.as_bytes(),
            ser.serialize_global(&global_record)?,
        );

        let stream_record = StreamRecord::build(
            &event.id,
            global,
            event.message_type.as_ref(),
            event.data.as_ref(),
            event.metadata.as_ref(),
        );
        batch.put_cf(
            db.stream(),
            stream_key.as_bytes(),
            ser.serialize_stream(&stream_record)?,
        );

        last_stream_pos = stream_pos;
        stream_pos = stream_pos.next();
    }

    // One atomic commit for the whole batch.
    db.write(batch)?;
    db.cached_global.fetch_max(global, Ordering::AcqRel);
    db.set_stream_head(stream_name, last_stream_pos.position());

    Ok(Position { global, stream: last_stream_pos })
}

/// Atomic multi-event append: one expected-version check, N records, one write
/// batch. Returns the [`Position`] of the LAST event (the new stream head).
pub fn write_messages(
    db: &DB,
    batch: WriteMessages,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    let first_stream_pos =
        resolve_first_stream_pos(db, batch.expected_version, &batch.stream_name)?;
    let base_global = get_last_global_position(db)?.0;
    write_batch_records(
        db,
        &batch.stream_name,
        base_global,
        first_stream_pos,
        &batch.events,
        ser,
    )
}

/// Single-event convenience wrapper over [`write_messages`].
pub fn write_mess(
    db: &DB,
    msg: WriteMessage,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    write_messages(db, msg.into(), ser)
}

/// Async multi-event append. For non-`Any` modes the global-head read and the
/// stream-head read run concurrently; `Any` skips the stream-head read.
pub async fn write_messages_async(
    db: Arc<DB>,
    batch: WriteMessages<'_>,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    let expected = batch.expected_version;
    let stream_name = batch.stream_name.to_string();

    let adb = Arc::clone(&db);
    let g = tokio::spawn(async move { get_last_global_position(&adb) });
    let adb = Arc::clone(&db);
    let s_stream = stream_name.clone();
    let s = tokio::spawn(async move {
        resolve_first_stream_pos(&adb, expected, &s_stream)
    });
    let (g, s) = tokio::join!(g, s);

    let base_global = g??.0;
    let first_stream_pos = s??;
    write_batch_records(
        &db,
        &batch.stream_name,
        base_global,
        first_stream_pos,
        &batch.events,
        ser,
    )
}

/// Single-event convenience wrapper over [`write_messages_async`].
pub async fn write_mess_async<'a>(
    db: Arc<DB>,
    msg: WriteMessage<'a>,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    write_messages_async(db, msg.into(), ser).await
}

#[cfg(test)]
mod test_global_key {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_from_bytes() {
        // Test case 1: Valid bytes
        let bytes: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let result = GlobalKey::from_bytes(bytes).unwrap();
        assert!(result == GlobalKey(1));

        // Test case 2: Invalid bytes (less than 8 bytes)
        let bytes: [u8; 4] = [0x00, 0x00, 0x00, 0x01];
        let result = GlobalKey::from_bytes(bytes).unwrap_err();
        assert!(matches!(result, Error::ParseKeyError));

        // Test case 3: Invalid bytes (more than 8 bytes)
        let bytes: [u8; 11] =
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = GlobalKey::from_bytes(bytes).unwrap_err();
        assert!(matches!(result, Error::ParseKeyError));
    }
}

#[cfg(test)]
mod test_get_last_stream_position {
    use super::*;
    use crate::rocks::db::test::SelfDestructingDB;
    use assert2::assert;

    #[test]
    fn it_works() {
        let db = SelfDestructingDB::new_tmp();
        let cf = db.stream();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(cf, b"s1|\x00\x00\x00\x00\x00\x00\x00\x20", []);
        batch.put_cf(cf, b"s2|\x00\x00\x00\x00\x00\x00\x00\x16", []);
        batch.put_cf(cf, b"s1|\x00\x00\x00\x00\x00\x00\x00\x30", []);
        batch.put_cf(cf, b"s2|\x00\x00\x00\x00\x00\x00\x00\x26", []);
        batch.put_cf(cf, b"s1|\x00\x00\x00\x00\x00\x00\x00\x10", []);
        batch.put_cf(cf, b"s2|\x00\x00\x00\x00\x00\x00\x00\x10", []);
        db.write(batch).unwrap();

        let res = get_last_stream_position(&db, "s1").unwrap();
        assert!(
            res == Some(StreamKey::new("s1".into(), StreamPos::decode(0x30)))
        );
        let res = get_last_stream_position(&db, "s2").unwrap();
        assert!(
            res == Some(StreamKey::new("s2".into(), StreamPos::decode(0x26)))
        );
    }

    #[test]
    fn it_returns_none_if_no_stream_records() {
        let db = SelfDestructingDB::new_tmp();
        let cf = db.stream();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(cf, b"s2|\x00\x00\x00\x00\x00\x00\x00\x20", []);
        batch.put_cf(cf, b"s3|\x00\x00\x00\x00\x00\x00\x00\x30", []);
        db.write(batch).unwrap();

        let res = get_last_stream_position(&db, "s1").unwrap();
        assert!(res == None);
    }
}

#[cfg(test)]
mod test_write_mess {
    use std::borrow::Cow;

    use assert2::assert;
    use ident::Id;

    use super::super::db::test::SelfDestructingDB;
    use super::*;
    use crate::write::WriteEvent;

    const fn ser() -> WriteSerializer {
        WriteSerializer::new()
    }

    fn event(payload: &[u8]) -> WriteEvent<'static> {
        WriteEvent {
            id: Id::new(),
            message_type: "T".into(),
            data: payload.to_vec().into(),
            metadata: Cow::Borrowed(b""),
        }
    }

    fn setup() -> SelfDestructingDB {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();

        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::NoStream,
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream2".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::NoStream,
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::Exact(StreamPos::new(0)),
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream2".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::Exact(StreamPos::new(0)),
        };
        write_mess(&db, msg, &mut ser).unwrap();
        db
    }

    #[rstest::rstest]
    fn it_writes_to_global_cf() {
        let db = setup();
        let bytes =
            db.get_cf(db.global(), u64::to_be_bytes(1)).unwrap().unwrap();

        // let x = rkyv::check_archived_root::<GlobalRecord>(&bytes[..]).unwrap();
        let x = GlobalRecord::from_bytes(&bytes).unwrap();

        assert!(x.stream_name == "stream1");
        assert!(x.message_type == "someMsgType");
        assert!(x.stream_position == 0);
    }

    #[rstest::rstest]
    fn it_writes_to_stream_cf() {
        let db = setup();
        let bytes = db
            .get_cf(
                db.stream(),
                StreamKey::new("stream1".into(), StreamPos::new(0))
                    .as_bytes(),
            )
            .unwrap()
            .unwrap();

        // let x = rkyv::check_archived_root::<StreamRecord>(&bytes[..]).unwrap();
        let x = StreamRecord::from_bytes(&bytes).unwrap();

        assert!(x.message_type == "someMsgType");
        assert!(x.global_position == 1);
    }

    #[rstest::rstest]
    fn it_writes_payloads_larger_than_the_initial_buffer() {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();
        let data = vec![7u8; 64 * 1024];
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "big".into(),
            message_type: "BigType".into(),
            data: data.clone().into(),
            metadata: Cow::Borrowed(b"{}"),
            expected_version: ExpectedVersion::NoStream,
        };
        write_mess(&db, msg, &mut ser).unwrap();

        let bytes =
            db.get_cf(db.global(), u64::to_be_bytes(1)).unwrap().unwrap();
        let rec = GlobalRecord::from_bytes(&bytes).unwrap();
        assert!(rec.data.len() == data.len());
        assert!(rec.data[..] == data[..]);
    }

    #[rstest::rstest]
    fn global_position_does_not_leak_across_db_instances() {
        // Regression: the last-global cache used to be a process-wide
        // `static mut`, so a fresh DB inherited another instance's position.
        let db1 = setup();
        let db2 = SelfDestructingDB::new_tmp();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1}"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::NoStream,
        };
        let mut ser = ser();
        let pos = write_mess(&db2, msg, &mut ser).unwrap();
        assert!(pos.global == 1);
        drop(db1);
    }

    #[rstest::rstest]
    fn writing_stream_pos_out_of_order_fails() {
        let db = SelfDestructingDB::new_tmp();
        let msg1 = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_version: ExpectedVersion::NoStream,
        };
        let mut msg2 = msg1.clone();
        msg2.expected_version = ExpectedVersion::Exact(StreamPos::new(0));
        let mut msg3 = msg1.clone();
        msg3.expected_version = ExpectedVersion::Exact(StreamPos::new(2));

        let mut ser = ser();
        write_mess(&db, msg1, &mut ser).unwrap();
        write_mess(&db, msg2, &mut ser).unwrap();
        let result = write_mess(&db, msg3, &mut ser).unwrap_err();
        assert!(let Error::WrongStreamPosition {
            stream: _,
            expected: Some(2),
            got: Some(1)
        } = result);
    }

    #[rstest::rstest]
    fn multi_event_append_is_atomic_and_contiguous() {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();
        let batch = WriteMessages {
            stream_name: "s1".into(),
            expected_version: ExpectedVersion::NoStream,
            events: (0..4).map(|i| event(&[i as u8])).collect(),
        };
        let pos = write_messages(&db, batch, &mut ser).unwrap();
        // Position reported is the last event's.
        assert!(pos.stream == StreamPos::new(3));
        assert!(pos.global == 4);

        // Every event landed at contiguous stream + global positions.
        for i in 0..4u64 {
            let sbytes = db
                .get_cf(
                    db.stream(),
                    StreamKey::new("s1".into(), StreamPos::new(i)).as_bytes(),
                )
                .unwrap()
                .unwrap();
            let srec = StreamRecord::from_bytes(&sbytes).unwrap();
            assert!(srec.global_position == i + 1);

            let gbytes = db
                .get_cf(db.global(), u64::to_be_bytes(i + 1))
                .unwrap()
                .unwrap();
            let grec = GlobalRecord::from_bytes(&gbytes).unwrap();
            assert!(grec.stream_position == i);
        }
    }

    /// Acceptance (bn-b2r): an `ExpectedVersion::Any` append performs ZERO disk
    /// stream-head reads. Asserted via real instrumentation
    /// (`DB::stream_head_reads`), not a comment.
    #[rstest::rstest]
    fn any_mode_append_does_zero_stream_head_reads() {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();

        // Prime the stream (warms the head cache). This NoStream append costs
        // exactly one stream-head read (its validation read).
        let prime = WriteMessages {
            stream_name: "s1".into(),
            expected_version: ExpectedVersion::NoStream,
            events: vec![event(b"prime")],
        };
        write_messages(&db, prime, &mut ser).unwrap();
        let reads_before = db.stream_head_reads();
        assert!(reads_before >= 1);

        // Any-mode multi-event append: must not touch the disk head at all.
        let batch = WriteMessages {
            stream_name: "s1".into(),
            expected_version: ExpectedVersion::Any,
            events: (0..3).map(|i| event(&[i as u8])).collect(),
        };
        let pos = write_messages(&db, batch, &mut ser).unwrap();

        assert!(db.stream_head_reads() == reads_before);
        // Events appended after the primed position 0 -> stream 1,2,3.
        assert!(pos.stream == StreamPos::new(3));
    }

    #[rstest::rstest]
    fn any_mode_on_cold_stream_starts_at_zero_without_reads() {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();
        let batch = WriteMessages {
            stream_name: "fresh".into(),
            expected_version: ExpectedVersion::Any,
            events: (0..2).map(|i| event(&[i as u8])).collect(),
        };
        let pos = write_messages(&db, batch, &mut ser).unwrap();
        assert!(db.stream_head_reads() == 0);
        assert!(pos.stream == StreamPos::new(1));
    }

    #[rstest::rstest]
    fn rejected_batch_writes_nothing() {
        let db = SelfDestructingDB::new_tmp();
        let mut ser = ser();
        // Exact(5) on an empty stream can never match -> whole batch rejected.
        let batch = WriteMessages {
            stream_name: "s1".into(),
            expected_version: ExpectedVersion::Exact(StreamPos::new(5)),
            events: (0..3).map(|i| event(&[i as u8])).collect(),
        };
        let err = write_messages(&db, batch, &mut ser).unwrap_err();
        assert!(let Error::WrongStreamPosition { .. } = err);

        // No record and no global-position advance.
        let landed = db
            .get_cf(
                db.stream(),
                StreamKey::new("s1".into(), StreamPos::new(0)).as_bytes(),
            )
            .unwrap();
        assert!(landed.is_none());
        assert!(get_last_global_position(&db).unwrap().0 == 0);
    }
}
