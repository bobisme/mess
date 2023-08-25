use std::borrow::Cow;

use super::db::DB;
use crate::{
    error::{Error, MessResult},
    write::WriteMessage,
    Position,
};
use rkyv::{Archive, Deserialize, Serialize};
use rocksdb::IteratorMode;

const SEPARATOR: u8 = '|' as u8;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// Derives can be passed through to the generated type:
#[archive_attr(derive(Debug))]
pub(crate) struct GlobalRecord {
    id: String,
    stream_name: String,
    stream_position: u64,
    message_type: String,
    data: Vec<u8>,
    metadata: Vec<u8>,
    ord: u64,
}

impl<D, M> TryFrom<&WriteMessage<'_, D, M>> for GlobalRecord
where
    D: ::serde::Serialize,
    M: ::serde::Serialize,
{
    type Error = Error;

    fn try_from(msg: &WriteMessage<'_, D, M>) -> Result<Self, Self::Error> {
        let data = ::serde_json::to_vec(&msg.data)?;
        let metadata = msg
            .metadata
            .as_ref()
            .map(|x| ::serde_json::to_vec(&x))
            .transpose()?
            .unwrap_or(vec![]);
        let stream_position =
            msg.expected_stream_position.map(|x| x + 1).unwrap_or(0);
        Ok(Self {
            id: msg.id.to_string(),
            stream_name: msg.stream_name.as_ref().into(),
            stream_position,
            message_type: msg.message_type.as_ref().into(),
            data: data.clone(),
            metadata: metadata.clone(),
            ord: 0,
        })
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// Derives can be passed through to the generated type:
#[archive_attr(derive(Debug))]
struct StreamRecord {
    global_position: u64,
    id: String,
    message_type: String,
    data: Vec<u8>,
    metadata: Vec<u8>,
    ord: u64,
}

impl StreamRecord {
    fn set_global_position(mut self, pos: u64) -> Self {
        self.global_position = pos;
        self
    }
}

impl<D, M> TryFrom<&WriteMessage<'_, D, M>> for StreamRecord
where
    D: ::serde::Serialize,
    M: ::serde::Serialize,
{
    type Error = Error;

    fn try_from(msg: &WriteMessage<'_, D, M>) -> Result<Self, Self::Error> {
        let data = ::serde_json::to_vec(&msg.data)?;
        let metadata = msg
            .metadata
            .as_ref()
            .map(|x| ::serde_json::to_vec(&x))
            .transpose()?
            .unwrap_or(vec![]);
        Ok(Self {
            global_position: 0,
            id: msg.id.to_string(),
            message_type: msg.message_type.as_ref().into(),
            data: data.clone(),
            metadata: metadata.clone(),
            ord: 0,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalKey(u64);

impl GlobalKey {
    pub fn new(position: u64) -> Self {
        GlobalKey(position)
    }

    pub fn as_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> MessResult<Self> {
        let position = u64::from_be_bytes(
            bytes.try_into().map_err(|_| Error::ParseKeyError)?,
        );
        Ok(GlobalKey(position))
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamKey<'a> {
    stream: Cow<'a, str>,
    position: u64,
}

impl<'a> StreamKey<'a> {
    pub fn new(stream: impl Into<Cow<'a, str>>, position: u64) -> Self {
        Self { stream: stream.into(), position }
    }

    pub fn max(stream: impl Into<Cow<'a, str>>) -> Self {
        Self { stream: stream.into(), position: u64::MAX }
    }

    pub fn next(&self) -> Self {
        Self { stream: self.stream.clone(), position: self.position + 1 }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = self.stream.as_bytes().to_vec();
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.position.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> MessResult<Self> {
        let (stream, sep_position) = bytes.split_at(bytes.len() - 9);
        if sep_position.len() != 9 || stream.is_empty() {
            return Err(Error::ParseKeyError);
        }
        if sep_position[0] != SEPARATOR {
            return Err(Error::ParseKeyError);
        }
        let position = &sep_position[1..];
        let position = u64::from_be_bytes(
            position.try_into().map_err(|_| Error::ParseKeyError)?,
        );
        Ok(StreamKey {
            stream: String::from_utf8(stream.to_vec())
                .map_err(|_| Error::ParseKeyError)?
                .into(),
            position,
        })
    }
}

pub fn get_last_global_position(db: &DB) -> MessResult<GlobalKey> {
    let last = db.iterator_cf(db.global(), IteratorMode::End).next();
    if last.is_none() {
        return Ok(GlobalKey::new(0));
    }
    last.unwrap().map(|(key, _)| GlobalKey::from_bytes(&key))?
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
        .next();
    if last.is_none() {
        return Ok(None);
    }
    let (key, _) = last.unwrap()?;
    let key = StreamKey::from_bytes(&key)?;
    Ok(Some(key))
}

pub fn write_mess<D: ::serde::Serialize, M: ::serde::Serialize>(
    db: &DB,
    msg: WriteMessage<D, M>,
) -> MessResult<Position> {
    let next_global = get_last_global_position(db)?.next();
    let last_stream = get_last_stream_position(db, &msg.stream_name)?;
    let next_stream = match (msg.expected_stream_position, last_stream) {
        (None, None) => Ok(StreamKey::new(msg.stream_name.as_ref(), 0)),
        (Some(a), Some(key)) if a == key.position => Ok(key.next()),
        (expected, key) => Err(Error::WrongStreamPosition {
            stream: msg.stream_name.to_string(),
            expected,
            got: key.map(|k| k.position),
        }),
    }?;

    let global_record = GlobalRecord::try_from(&msg)?;

    let global_bytes = rkyv::to_bytes::<_, 1024>(&global_record)
        .map_err(|e| Error::SerError(e.to_string()))?;

    let stream_record =
        StreamRecord::try_from(&msg)?.set_global_position(next_global.0);

    let stream_bytes = rkyv::to_bytes::<_, 1024>(&stream_record)
        .map_err(|e| Error::SerError(e.to_string()))?;

    let mut batch = rocksdb::WriteBatch::default();
    batch.put_cf(db.global(), next_global.as_bytes(), &global_bytes);
    batch.put_cf(db.stream(), next_stream.as_bytes(), &stream_bytes);
    db.write(batch)?;

    Ok(Position { global: next_global.0, stream: Some(next_stream.position) })
}

#[cfg(test)]
mod test_global_key {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_from_bytes() {
        // Test case 1: Valid bytes
        let bytes: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];
        let result = GlobalKey::from_bytes(&bytes).unwrap();
        assert_eq!(result, GlobalKey(1));

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
        assert_eq!(res, Some(StreamKey::new("s1", 0x30)));
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
        assert_eq!(res, None);
    }
}

#[cfg(test)]
mod test_stream_key {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_as_bytes() {
        let key = StreamKey::new("somestream", 70);
        let bytes = key.as_bytes();
        assert_eq!(bytes, b"somestream|\x00\x00\x00\x00\x00\x00\x00\x46");
    }

    mod from_bytes {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn it_works() {
            // Test case 1: Valid input
            let bytes = b"test_stream|\x00\x00\x00\x00\x00\x00\x00\x0D";
            let expected_result =
                StreamKey { stream: "test_stream".into(), position: 13 };
            assert_eq!(StreamKey::from_bytes(bytes).unwrap(), expected_result);
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
    use assert2::assert;
    use ident::Id;
    use serde_json::json;

    use super::super::db::test::SelfDestructingDB;
    use super::*;

    fn setup() -> SelfDestructingDB {
        let db = SelfDestructingDB::new_tmp();

        let msg = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: json!({"a": 1}),
            metadata: Some(json!({"b": 2})),
            expected_stream_position: None,
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
            .get_cf(db.stream(), StreamKey::new("stream1", 0).as_bytes())
            .unwrap()
            .unwrap();

        let x = rkyv::check_archived_root::<StreamRecord>(&bytes[..]).unwrap();

        assert!(x.message_type == "someMsgType");
        assert!(x.global_position == 1);
    }

    #[rstest::rstest]
    fn writing_stream_pos_out_of_order_fails() {
        let db = SelfDestructingDB::new_tmp();
        let msg1 = WriteMessage {
            id: Id::new(),
            stream_name: "stream1".into(),
            message_type: "someMsgType".into(),
            data: json!({"a": 1}),
            metadata: Some(json!({"b": 2})),
            expected_stream_position: None,
        };
        let mut msg2 = msg1.clone();
        msg2.expected_stream_position = Some(0);
        let mut msg3 = msg1.clone();
        msg3.expected_stream_position = Some(2);

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
