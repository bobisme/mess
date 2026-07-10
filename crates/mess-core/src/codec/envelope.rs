//! [`StoredEvent`]: the decoded-header, still-encoded-payload shape handed
//! back by the log for one event — what the upcaster layer consumes.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::error::CodecError;
use super::msgpack::{CODEC_ID_MSGPACK_NAMED, decode_payload, encode_payload};

/// One event as read from the log: name and per-event header fields
/// decoded, payload bytes still encoded under `codec_id`.
///
/// Field widths mirror the on-disk `EventSubframe` header
/// (docs/spec/01-log-format.md §"EventSubframe header"):
/// `schema_version: u16`, `codec_id: u16`. `event_name` stands in here for
/// the wire's interned `event_type_id: u32`
/// (docs/spec/04-registry.md) — name resolution is a registry concern
/// outside this self-contained codec layer; callers that have already
/// resolved `event_type_id` to a name construct this directly.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoredEvent {
    /// The event's registered name (e.g. `"trip.completed"`).
    pub event_name:     String,
    /// Schema version of the payload at write time. Paired with
    /// `event_name` this is the upcaster dispatch key.
    pub schema_version: u16,
    /// Which codec `payload` is encoded under. `0` = bootstrap (frozen,
    /// not handled by this layer); `1` = msgpack-named.
    pub codec_id:       u16,
    /// The still-encoded event payload.
    pub payload:        Vec<u8>,
}

impl StoredEvent {
    /// Build a `StoredEvent` by encoding `value` under `codec_id 1`
    /// (msgpack-named) — the write path always writes the latest
    /// `schema_version` under the one codec this layer implements.
    pub fn encode<T: Serialize>(
        event_name: impl Into<String>,
        schema_version: u16,
        value: &T,
    ) -> Result<Self, CodecError> {
        let payload = encode_payload(CODEC_ID_MSGPACK_NAMED, value)?;
        Ok(Self {
            event_name: event_name.into(),
            schema_version,
            codec_id: CODEC_ID_MSGPACK_NAMED,
            payload,
        })
    }

    /// Decode `payload` as `T` under this envelope's `codec_id`.
    ///
    /// This does not check `event_name`/`schema_version` against a version
    /// chain — callers that need upcast dispatch use the `event_versions!`
    /// macro's generated `decode_*` function instead, which calls this and
    /// adds that routing.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, CodecError> {
        decode_payload(self.codec_id, &self.payload)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::error::CodecError;
    use crate::codec::msgpack::CODEC_ID_BOOTSTRAP;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Sample {
        id:   u32,
        name: String,
    }

    #[test]
    fn encode_then_decode_round_trips() {
        let v = Sample { id: 1, name: "a".into() };
        let ev = StoredEvent::encode("sample", 1, &v).unwrap();
        assert_eq!(ev.event_name, "sample");
        assert_eq!(ev.schema_version, 1);
        assert_eq!(ev.codec_id, CODEC_ID_MSGPACK_NAMED);
        let back: Sample = ev.decode().unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn decode_reports_unknown_codec_id() {
        let ev = StoredEvent {
            event_name:     "sample".into(),
            schema_version: 1,
            codec_id:       200,
            payload:        vec![],
        };
        let err = ev.decode::<Sample>().unwrap_err();
        assert!(matches!(err, CodecError::UnknownCodecId { codec_id: 200 }));
    }

    #[test]
    fn decode_reports_bootstrap_codec_as_unsupported_here() {
        let ev = StoredEvent {
            event_name:     "sample".into(),
            schema_version: 1,
            codec_id:       CODEC_ID_BOOTSTRAP,
            payload:        vec![],
        };
        let err = ev.decode::<Sample>().unwrap_err();
        assert!(matches!(err, CodecError::BootstrapCodecUnsupported));
    }
}
