use std::sync::{atomic::Ordering, Arc};

use super::{
    db::DB,
    keys::{GlobalKey, StreamKey},
    record::{GlobalRecord, StreamRecord},
};
use crate::{
    error::{Error, Result},
    write::{WriteMessage, WriteSerialMessage},
    Position, StreamPos,
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

fn next_stream_pos<'a>(
    expected_position: Option<StreamPos>,
    stream_name: &'a str,
    last_stream: Option<StreamKey<'a>>,
) -> Result<StreamKey<'a>> {
    match (expected_position, last_stream) {
        (None, None) => {
            Ok(StreamKey::new(stream_name.into(), StreamPos::new(0)))
        }
        (Some(a), Some(key)) if a == key.position => Ok(key.next()),
        (expected, key) => Err(Error::WrongStreamPosition {
            stream: stream_name.to_string(),
            expected: expected.map(|x| x.position()),
            got: key.map(|k| k.position.position()),
        }),
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

fn write_records(
    db: &DB,
    msg: WriteSerialMessage,
    next_global: GlobalKey,
    next_stream: StreamKey,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    let global_record = GlobalRecord::from_write_serial_message(&msg)?;
    let stream_record =
        StreamRecord::from_write_serial_message(&msg, next_global.0)?
            .set_global_position(next_global.0);

    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(
        db.global(),
        next_global.as_bytes(),
        ser.serialize_global(&global_record)?,
    );
    batch.put_cf(
        db.stream(),
        next_stream.as_bytes(),
        ser.serialize_stream(&stream_record)?,
    );
    db.write(batch)?;
    db.cached_global.fetch_max(next_global.0, Ordering::AcqRel);

    Ok(Position { global: next_global.0, stream: next_stream.position })
}
pub fn write_mess(
    db: &DB,
    msg: WriteMessage,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    write_serial_mess(db, msg.into(), ser)
}

pub fn write_serial_mess(
    db: &DB,
    msg: WriteSerialMessage,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    let next_global = get_last_global_position(db)?.next();
    let last_stream = get_last_stream_position(db, &msg.stream_name)?;
    let stream_name = msg.stream_name.clone();
    let next_stream =
        next_stream_pos(msg.expected_position, &stream_name, last_stream)?;
    write_records(db, msg, next_global, next_stream, ser)
}
pub async fn write_mess_async<'a>(
    db: Arc<DB>,
    msg: WriteMessage<'a>,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    write_serial_mess_async(db, msg.into(), ser).await
}

pub async fn write_serial_mess_async<'a>(
    db: Arc<DB>,
    msg: WriteSerialMessage<'a>,
    ser: &mut WriteSerializer,
) -> Result<Position> {
    let (last_global, last_stream) = {
        let adb = Arc::clone(&db);
        let g = tokio::spawn(async move { get_last_global_position(&adb) });
        let adb = Arc::clone(&db);
        let stream_name = msg.stream_name.to_string();
        let s = tokio::spawn(async move {
            get_last_stream_position(&adb, &stream_name)
        });
        tokio::join!(g, s)
    };
    let next_global = last_global??.next();
    let stream_name = msg.stream_name.clone();
    let next_stream =
        next_stream_pos(msg.expected_position, &stream_name, last_stream??)?;
    write_records(&db, msg, next_global, next_stream, ser)
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

    const fn ser() -> WriteSerializer {
        WriteSerializer::new()
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
            expected_stream_position: None,
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream2".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_stream_position: None,
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_stream_position: Some(StreamPos::new(0)),
        };
        write_mess(&db, msg, &mut ser).unwrap();
        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream2".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_stream_position: Some(StreamPos::new(0)),
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
            expected_stream_position: None,
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
            expected_stream_position: None,
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
            expected_stream_position: None,
        };
        let mut msg2 = msg1.clone();
        msg2.expected_stream_position = Some(StreamPos::new(0));
        let mut msg3 = msg1.clone();
        msg3.expected_stream_position = Some(StreamPos::new(2));

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
}
