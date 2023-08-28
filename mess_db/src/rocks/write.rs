use std::sync::Arc;

use super::{
    db::DB,
    record::{GlobalKey, GlobalRecord, StreamKey, StreamRecord},
};
use crate::{
    error::{Error, MessResult},
    write::WriteSerialMessage,
    Position, StreamPos,
};
use rocksdb::IteratorMode;

pub fn get_last_global_position(db: &DB) -> MessResult<GlobalKey> {
    let last = db.iterator_cf(db.global(), IteratorMode::End).next();
    if last.is_none() {
        return Ok(GlobalKey::new(0));
    }
    last.unwrap()
        .map(|(key, _)| GlobalKey::from_bytes(&key))
        .map_err(|e| Error::ReadError(e.to_string()))?
        .map_err(|e| Error::ReadError(e.to_string()))
}

pub fn get_last_stream_position<'a>(
    db: &DB,
    stream: &str,
) -> MessResult<Option<StreamKey<'a>>> {
    let last = db
        .iterator_cf(
            db.stream(),
            IteratorMode::From(
                &StreamKey::max(stream).as_bytes(),
                rocksdb::Direction::Reverse,
            ),
        )
        .next()
        .transpose()?;
    let Some((key, _)) = last else {
        return Ok(None);
    };
    StreamKey::from_bytes(&key).map(|x| Some(x))
}

fn next_stream_pos<'a>(
    expected_position: Option<StreamPos>,
    stream_name: &'a str,
    last_stream: Option<StreamKey<'a>>,
) -> Result<StreamKey<'a>, Error> {
    match (expected_position, last_stream) {
        (None, None) => Ok(StreamKey::new(stream_name, StreamPos::Serial(0))),
        (Some(a), Some(key)) if a == key.position => Ok(key.next()),
        (expected, key) => Err(Error::WrongStreamPosition {
            stream: stream_name.to_string(),
            expected: expected.map(|x| x.position()),
            got: key.map(|k| k.position.position()),
        }),
    }
}

fn write_records(
    db: &DB,
    msg: WriteSerialMessage,
    next_global: GlobalKey,
    next_stream: StreamKey,
) -> MessResult<Position> {
    let mut global_record = GlobalRecord::from_write_serial_message(&msg)?;
    let mut stream_record =
        StreamRecord::from_write_serial_message(&msg, next_global.0)?
            .set_global_position(next_global.0);

    let global_bytes = rkyv::to_bytes::<_, 1024>(&global_record)
        .map_err(|e| Error::SerError(e.to_string()))?;

    let stream_bytes = rkyv::to_bytes::<_, 1024>(&stream_record)
        .map_err(|e| Error::SerError(e.to_string()))?;

    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(db.global(), next_global.as_bytes(), &global_bytes);
    batch.put_cf(db.stream(), next_stream.as_bytes(), &stream_bytes);
    db.write(batch)?;

    Ok(Position { global: next_global.0, stream: next_stream.position })
}

pub fn write_mess(db: &DB, msg: WriteSerialMessage) -> MessResult<Position> {
    let next_global = get_last_global_position(db)?.next();
    let last_stream = get_last_stream_position(db, &msg.stream_name)?;
    let stream_name = msg.stream_name.to_owned();
    let next_stream =
        next_stream_pos(msg.expected_position, &stream_name, last_stream)?;
    write_records(db, msg, next_global, next_stream)

    // {
    //     let mut dbuf = db.data_buffer();
    //     let mut mbuf = db.meta_buffer();
    //     let mut data_serializer = serde_json::Serializer::new(&mut *dbuf);
    //     let mut metadata_serializer = serde_json::Serializer::new(&mut *mbuf);
    //     msg.data.serialize(&mut data_serializer)?;
    //     msg.metadata
    //         .as_ref()
    //         .map(|x| x.serialize(&mut metadata_serializer))
    //         .transpose()?;
    // }
    // db.clear_serialization_buffers();
}

pub async fn write_mess_async<'a>(
    db: Arc<DB>,
    msg: WriteSerialMessage<'a>,
) -> MessResult<Position> {
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
    let stream_name = msg.stream_name.to_owned();
    let next_stream =
        next_stream_pos(msg.expected_position, &stream_name, last_stream??)?;
    write_records(&db, msg, next_global, next_stream)
}

#[cfg(test)]
mod test_global_key {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_from_bytes() {
        // Test case 1: Valid bytes
        let bytes: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let result = GlobalKey::from_bytes(&bytes).unwrap();
        assert!(result == GlobalKey(1));

        // Test case 2: Invalid bytes (less than 8 bytes)
        let bytes: [u8; 4] = [0x00, 0x00, 0x00, 0x01];
        let result = GlobalKey::from_bytes(&bytes).unwrap_err();
        assert!(matches!(result, Error::ParseKeyError));

        // Test case 3: Invalid bytes (more than 8 bytes)
        let bytes: [u8; 11] =
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = GlobalKey::from_bytes(&bytes).unwrap_err();
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
        batch.put_cf(cf, b"s1|\x00\x00\x00\x00\x00\x00\x00\x30", []);
        batch.put_cf(cf, b"s1|\x00\x00\x00\x00\x00\x00\x00\x10", []);
        db.write(batch).unwrap();

        let res = get_last_stream_position(&db, "s1").unwrap();
        assert!(res == Some(StreamKey::new("s1", StreamPos::from_store(0x30))));
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
mod test_stream_key {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_as_bytes() {
        let key = StreamKey::new("somestream", StreamPos::Serial(13));
        let bytes = key.as_bytes();
        assert!(bytes == b"somestream|\x00\x00\x00\x00\x00\x00\x00\x1A");
    }

    mod from_bytes {
        use super::*;
        use assert2::assert;

        #[test]
        fn it_works() {
            // Test case 1: Valid input
            let bytes = b"test_stream|\x00\x00\x00\x00\x00\x00\x00\x1A";
            let expected_result = StreamKey {
                stream: "test_stream".into(),
                position: StreamPos::Serial(13),
            };
            assert!(StreamKey::from_bytes(bytes).unwrap() == expected_result);
        }

        #[test]
        fn it_fails_if_key_too_short() {
            let bytes = b"|\x00\x00\x00\x00\x00\x00\x00\xFF";
            assert!(matches!(
                StreamKey::from_bytes(bytes).unwrap_err(),
                Error::ParseKeyError
            ));
        }

        #[test]
        fn it_fails_if_no_separator() {
            let bytes = b"invalid_\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
            assert!(matches!(
                StreamKey::from_bytes(bytes).unwrap_err(),
                Error::ParseKeyError
            ));
        }
    }
}

#[cfg(test)]
mod test_write_mess {
    use std::borrow::Cow;

    use assert2::assert;
    use ident::Id;

    use super::super::db::test::SelfDestructingDB;
    use super::*;

    fn setup() -> SelfDestructingDB {
        let db = SelfDestructingDB::new_tmp();

        let msg = WriteSerialMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_position: None,
        };
        write_mess(&db, msg).unwrap();
        db
    }

    #[rstest::rstest]
    fn it_writes_to_global_cf() {
        let db = setup();
        let bytes =
            db.get_cf(db.global(), u64::to_be_bytes(1)).unwrap().unwrap();

        let x = rkyv::check_archived_root::<GlobalRecord>(&bytes[..]).unwrap();

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
                StreamKey::new("stream1", StreamPos::Serial(0)).as_bytes(),
            )
            .unwrap()
            .unwrap();

        let x = rkyv::check_archived_root::<StreamRecord>(&bytes[..]).unwrap();

        assert!(x.message_type == "someMsgType");
        assert!(x.global_position == 1);
    }

    #[rstest::rstest]
    fn writing_stream_pos_out_of_order_fails() {
        let db = SelfDestructingDB::new_tmp();
        let msg1 = WriteSerialMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: Cow::Borrowed(b"{\"a\": 1})"),
            metadata: Cow::Borrowed(b"{\"b\": 2}"),
            expected_position: None,
        };
        let mut msg2 = msg1.clone();
        msg2.expected_position = Some(StreamPos::Serial(0));
        let mut msg3 = msg1.clone();
        msg3.expected_position = Some(StreamPos::Serial(2));

        write_mess(&db, msg1).unwrap();
        write_mess(&db, msg2).unwrap();
        let result = write_mess(&db, msg3).unwrap_err();
        assert!(let Error::WrongStreamPosition {
            stream: _,
            expected: Some(2),
            got: Some(1)
        } = result);
    }
}
