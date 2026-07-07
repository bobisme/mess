//! Uniform wrapper over the candidate payload codecs.

use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Codec {
    /// serde_json — UTF-8 text, field names in every record.
    Json,
    /// ciborium — CBOR, self-describing, field names as map keys.
    Cbor,
    /// rmp-serde `to_vec_named` — MessagePack maps, field names as keys,
    /// enum variants as strings.
    MsgpackNamed,
    /// rmp-serde `to_vec` — MessagePack arrays, positional fields,
    /// enum variants as integer indices.
    MsgpackCompact,
    /// postcard — varint, positional, no framing metadata at all.
    Postcard,
    /// bincode 1.x serde mode — fixed-width ints, positional.
    Bincode,
}

pub const ALL_CODECS: [Codec; 6] = [
    Codec::Json,
    Codec::Cbor,
    Codec::MsgpackNamed,
    Codec::MsgpackCompact,
    Codec::Postcard,
    Codec::Bincode,
];

impl Codec {
    pub fn name(self) -> &'static str {
        match self {
            Codec::Json => "json",
            Codec::Cbor => "cbor",
            Codec::MsgpackNamed => "msgpack-named",
            Codec::MsgpackCompact => "msgpack-compact",
            Codec::Postcard => "postcard",
            Codec::Bincode => "bincode",
        }
    }

    pub fn encode<T: Serialize>(self, v: &T) -> Vec<u8> {
        match self {
            Codec::Json => serde_json::to_vec(v).expect("json encode"),
            Codec::Cbor => {
                let mut buf = Vec::new();
                ciborium::into_writer(v, &mut buf).expect("cbor encode");
                buf
            }
            Codec::MsgpackNamed => rmp_serde::to_vec_named(v).expect("msgpack-named encode"),
            Codec::MsgpackCompact => rmp_serde::to_vec(v).expect("msgpack-compact encode"),
            Codec::Postcard => postcard::to_stdvec(v).expect("postcard encode"),
            Codec::Bincode => bincode::serialize(v).expect("bincode encode"),
        }
    }

    pub fn decode<T: DeserializeOwned>(self, bytes: &[u8]) -> Result<T, String> {
        match self {
            Codec::Json => serde_json::from_slice(bytes).map_err(|e| e.to_string()),
            Codec::Cbor => ciborium::from_reader(bytes).map_err(|e| e.to_string()),
            Codec::MsgpackNamed | Codec::MsgpackCompact => {
                rmp_serde::from_slice(bytes).map_err(|e| e.to_string())
            }
            Codec::Postcard => postcard::from_bytes(bytes).map_err(|e| e.to_string()),
            Codec::Bincode => bincode::deserialize(bytes).map_err(|e| e.to_string()),
        }
    }
}
