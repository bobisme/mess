//! Upcaster prototype: stored (event_name, schema_version, codec_id, payload)
//! -> decode as that version's struct -> upcast chain -> latest type.
//!
//! The `event_versions!` macro at the bottom is the shape a
//! `#[derive(EventVersions)]` / `#[event(version = N)]` derive would generate.

use crate::codecs::Codec;
use serde::{Deserialize, Serialize};

// ------------------------------------------------------------- frame model

/// What the log hands back for one event (payload already unframed,
/// decompressed, CRC-checked by lower layers).
#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub event_name: String,
    pub schema_version: u16,
    pub codec_id: u8,
    pub payload: Vec<u8>,
}

/// codec_id registry (codec 0 is the frozen bootstrap codec per D3;
/// payload codecs start at 1).
pub const CODEC_ID_MSGPACK_NAMED: u8 = 1;

pub fn codec_for_id(id: u8) -> Result<Codec, DecodeError> {
    match id {
        CODEC_ID_MSGPACK_NAMED => Ok(Codec::MsgpackNamed),
        other => Err(DecodeError::UnknownCodec(other)),
    }
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    UnknownCodec(u8),
    UnknownVersion { event_name: String, version: u16 },
    WrongEventName { expected: &'static str, got: String },
    Codec(String),
}

// -------------------------------------------------- the 3-version event

/// V1: driver referenced by free-text name, distance in miles,
/// timestamp in whole seconds.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct TripCompletedV1 {
    pub trip_id: u64,
    pub driver: String,
    pub distance_miles: f64,
    pub completed_at: i64, // unix seconds
}

/// V2: semantic migration #1 — field rename: `driver` -> `driver_name`
/// (rename happens in the upcaster, NOT via #[serde(rename)]; old bytes
/// still say "driver" and only V1's struct ever reads them).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct TripCompletedV2 {
    pub trip_id: u64,
    pub driver_name: String,
    pub distance_miles: f64,
    pub completed_at: i64, // unix seconds
}

/// V3: semantic migration #2 — unit changes (miles -> meters,
/// seconds -> milliseconds) plus an additive optional field.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct TripCompletedV3 {
    pub trip_id: u64,
    pub driver_name: String,
    pub distance_m: f64,
    pub completed_at_ms: i64,
    pub rating: Option<u8>,
}

pub const METERS_PER_MILE: f64 = 1609.344;

// ------------------------------------------------------------ upcast chain

/// One hop in the version chain. A derive would let users write only these.
pub trait Upcast<Prev> {
    fn upcast(prev: Prev) -> Self;
}

impl Upcast<TripCompletedV1> for TripCompletedV2 {
    fn upcast(v1: TripCompletedV1) -> Self {
        TripCompletedV2 {
            trip_id: v1.trip_id,
            driver_name: v1.driver, // the rename, expressed in code
            distance_miles: v1.distance_miles,
            completed_at: v1.completed_at,
        }
    }
}

impl Upcast<TripCompletedV2> for TripCompletedV3 {
    fn upcast(v2: TripCompletedV2) -> Self {
        TripCompletedV3 {
            trip_id: v2.trip_id,
            driver_name: v2.driver_name,
            distance_m: v2.distance_miles * METERS_PER_MILE, // unit change
            completed_at_ms: v2.completed_at * 1000,         // unit change
            rating: None,
        }
    }
}

// --------------------------------------------- what the derive generates

/// Transitive upcast to a fixed latest type. Generated, never hand-written.
pub trait UpcastsTo<Latest> {
    fn upcast_to_latest(self) -> Latest;
}

/// Composes the one-hop `Upcast` impls into transitive `UpcastsTo<Latest>`
/// impls by walking the version list pairwise: version N upcasts one hop to
/// N+1, then delegates to N+1's generated impl. The last version is Latest
/// and gets the identity impl.
macro_rules! upcast_impls {
    ($latest:ty; $cur:ty, $next:ty $(, $rest:ty)*) => {
        impl UpcastsTo<$latest> for $cur {
            fn upcast_to_latest(self) -> $latest {
                <$next as Upcast<$cur>>::upcast(self).upcast_to_latest()
            }
        }
        upcast_impls!($latest; $next $(, $rest)*);
    };
    ($latest:ty; $cur:ty) => {
        impl UpcastsTo<$latest> for $cur {
            fn upcast_to_latest(self) -> $latest {
                self // $cur == $latest here
            }
        }
    };
}

/// Generates:
///  - `UpcastsTo<Latest>` impls for every version (composing the one-hop
///    `Upcast` impls the user wrote),
///  - `decode_to_latest(&StoredEvent) -> Result<Latest, DecodeError>`
///    dispatching on the stored schema_version.
macro_rules! event_versions {
    (
        name: $event_name:literal,
        latest: $latest:ty,
        versions: [ $( $ver:literal => $ty:ty ),+ $(,)? ],
        decode_fn: $decode_fn:ident
    ) => {
        upcast_impls!($latest; $($ty),+);

        pub fn $decode_fn(ev: &StoredEvent) -> Result<$latest, DecodeError> {
            if ev.event_name != $event_name {
                return Err(DecodeError::WrongEventName {
                    expected: $event_name,
                    got: ev.event_name.clone(),
                });
            }
            let codec = codec_for_id(ev.codec_id)?;
            match ev.schema_version {
                $(
                    $ver => {
                        let v: $ty =
                            codec.decode(&ev.payload).map_err(DecodeError::Codec)?;
                        Ok(v.upcast_to_latest())
                    }
                )+
                other => Err(DecodeError::UnknownVersion {
                    event_name: ev.event_name.clone(),
                    version: other,
                }),
            }
        }
    };
}

event_versions! {
    name: "trip.completed",
    latest: TripCompletedV3,
    versions: [ 1 => TripCompletedV1, 2 => TripCompletedV2, 3 => TripCompletedV3 ],
    decode_fn: decode_trip_completed
}

// -------------------------------------------------------------- helpers

/// Encode a value as a StoredEvent (what the write path does; always writes
/// the latest schema_version).
pub fn store<T: Serialize>(event_name: &str, schema_version: u16, value: &T) -> StoredEvent {
    StoredEvent {
        event_name: event_name.to_string(),
        schema_version,
        codec_id: CODEC_ID_MSGPACK_NAMED,
        payload: Codec::MsgpackNamed.encode(value),
    }
}
