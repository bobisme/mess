use std::borrow::Cow;

use crate::error::Error;
use crate::error::Result;
use crate::StreamPos;

pub(crate) const SEPARATOR: u8 = b'|';
pub(crate) const SEPARATOR_CHAR: char = '|';

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalKey(pub(crate) u64);

impl GlobalKey {
    #[must_use]
    pub const fn new(position: u64) -> Self {
        GlobalKey(position)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        let position = u64::from_be_bytes(
            bytes.as_ref().try_into().map_err(|_| Error::ParseKeyError)?,
        );
        Ok(GlobalKey(position))
    }

    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamKey<'a> {
    pub(crate) stream: Cow<'a, str>,
    pub(crate) position: StreamPos,
}

impl<'a> StreamKey<'a> {
    #[must_use]
    pub const fn new(stream: Cow<'a, str>, position: StreamPos) -> Self {
        Self { stream, position }
    }

    #[must_use]
    pub const fn max(stream: Cow<'a, str>) -> Self {
        Self { stream, position: StreamPos::Relaxed(u64::MAX) }
    }

    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn next(self) -> Self {
        Self { stream: self.stream, position: self.position.next() }
    }

    #[must_use]
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = self.stream.as_bytes().to_vec();
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.position.encode().to_be_bytes());
        bytes
    }

    #[must_use]
    pub fn to_bytes(self) -> Vec<u8> {
        self.as_bytes()
    }

    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        let (stream, sep_position) =
            bytes.as_ref().split_at(bytes.as_ref().len() - 9);
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
            position: StreamPos::decode(position),
        })
    }
}

// Compile-time tests.
const _: () = {};

#[cfg(test)]
mod test_global_key {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_as_bytes() {
        let key = GlobalKey::new(10);
        let bytes = key.as_bytes();
        assert!(bytes == [0, 0, 0, 0, 0, 0, 0, 10]);
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [0, 0, 0, 0, 0, 0, 0, 10];
        let key = GlobalKey::from_bytes(bytes).unwrap();
        assert!(key.0 == 10);
    }

    #[test]
    fn test_next() {
        let key = GlobalKey::new(10);
        let next_key = key.next();
        assert!(next_key.0 == 11);
    }
}

#[cfg(test)]
mod test_stream_key {
    use super::*;
    use assert2::assert;

    #[test]
    fn test_as_bytes() {
        let key =
            StreamKey::new("somestream".into(), StreamPos::Sequential(13));
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
                position: StreamPos::Sequential(13),
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
