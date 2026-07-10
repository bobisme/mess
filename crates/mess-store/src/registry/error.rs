//! [`RegistryError`]: the typed failure surface for codec-0 decode and
//! REG-rule enforcement (`docs/spec/04-registry.md`).
//!
//! Two families of failure live here, deliberately kept in one enum because
//! both can occur while replaying `$registry` (§7.2) and a caller generally
//! wants to treat either as "this log/record is not trustworthy":
//!
//! - **decode corruption** — the bytes are not a legal `codec_id 0` payload at
//!   all (REG7/REG8's frozen tables reject anything else), and
//! - **REG-rule violations** — the bytes decode fine but describe a state
//!   transition the spec forbids (double registration, an unregistered
//!   reference, a reserved ID being targeted, ...).

use std::fmt;

/// Why a `codec_id 0` payload or a `$registry` state transition was rejected.
///
/// `E` is the backend's own error type, threaded through so a caller working
/// against `Registry<B>` gets one error type instead of two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError<E = std::convert::Infallible> {
    // -- decode corruption (REG7/REG8) --------------------------------
    /// The payload was shorter than the fixed-offset fields for its
    /// `record_kind` require.
    PayloadTooShort {
        /// Which record kind was being decoded (or `None` if the payload was
        /// empty, so not even the tag byte was present).
        record_kind: Option<u8>,
        /// Bytes needed at minimum.
        need:        usize,
        /// Bytes actually present.
        got:         usize,
    },
    /// `record_kind` byte was `0x00` or `>= 0x06` (REG8: closed, frozen tag
    /// set).
    UnknownRecordKind(u8),
    /// A `str` field's bytes were not well-formed UTF-8 (§3.1).
    InvalidUtf8 {
        /// Which record kind carried the bad string.
        record_kind: u8,
    },
    /// A length-prefixed field (`str`/`blob`) claimed more bytes than the
    /// payload actually has.
    TrailingLengthMismatch {
        /// Which record kind was being decoded.
        record_kind: u8,
        /// Bytes the length prefix promised.
        need:        usize,
        /// Bytes actually remaining in the payload.
        got:         usize,
    },

    // -- REG-rule violations (§4, §5, §6) -----------------------------
    /// REG2: a `*Registered` record was seen for a reserved ID (always `0`
    /// in its namespace) — never legal, since the four reserved IDs are
    /// defined by spec text, not by any log event.
    ReservedIdRegistered {
        /// The record kind that carried the reserved ID.
        record_kind: u8,
    },
    /// REG14: a second `*Registered` record was seen for an ID already
    /// present in the materialized table.
    AlreadyRegistered {
        /// Which namespace (`"stream"`, `"category"`, `"event_type"`,
        /// `"dict"`).
        namespace: &'static str,
        /// The ID that was registered twice.
        id:        u64,
    },
    /// REG12: a record referenced an ID (as a category, a scope, or an
    /// alias target) that is not yet visible in replay order.
    UnregisteredReference {
        /// Which namespace the missing ID lives in.
        namespace: &'static str,
        /// The missing ID.
        id:        u64,
    },
    /// REG17: a `NameAliased` record targeted a reserved ID (`0`).
    ReservedIdTargeted {
        /// Which namespace (`target_kind`) was targeted.
        namespace: &'static str,
    },
    /// REG16: a name was bound to a different ID than the one it is already
    /// permanently bound to in this namespace.
    NameAlreadyBound {
        /// Which namespace the name lives in.
        namespace: &'static str,
        /// The name in question.
        name:      String,
    },
    /// `NameAliased.target_kind` was `0` or `>= 4` (§3.7).
    InvalidTargetKind(u8),
    /// D-REG-E: an `event_type` alias's `target_id` had nonzero high 32
    /// bits — the zero-extension rule was violated.
    NonZeroHighBits {
        /// The raw 64-bit `target_id` as decoded.
        target_id: u64,
    },
    /// D-REG-E: a `DictRegistered { scope_kind: TARGET_KIND_EVENT_TYPE, .. }`
    /// record's `scope_id` had nonzero high 32 bits — the same
    /// zero-extension rule as [`NonZeroHighBits`](Self::NonZeroHighBits),
    /// but for the dictionary scope field rather than an alias target.
    ScopeIdNonZeroHighBits {
        /// The raw 64-bit `scope_id` as decoded.
        scope_id: u64,
    },
    /// REG9/REG20: a declared `codec_id` was `0`, which is reserved to
    /// `$registry`'s own bootstrap format and may never be claimed by a
    /// domain event type or a dictionary.
    ReservedCodecId {
        /// Which record kind declared it.
        record_kind: u8,
    },
    /// `DictRegistered.scope_kind` was not `2` (category) or `3`
    /// (event_type) (§3.8).
    InvalidScopeKind(u8),

    /// The underlying backend failed.
    Backend(E),
}

impl<E: fmt::Display> fmt::Display for RegistryError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::PayloadTooShort { record_kind, need, got } => {
                write!(
                    f,
                    "registry payload too short (record_kind \
                     {record_kind:?}): need {need}, got {got}"
                )
            }
            RegistryError::UnknownRecordKind(k) => {
                write!(f, "unknown/reserved registry record_kind {k:#04x}")
            }
            RegistryError::InvalidUtf8 { record_kind } => write!(
                f,
                "invalid UTF-8 in registry record_kind {record_kind:#04x}"
            ),
            RegistryError::TrailingLengthMismatch {
                record_kind,
                need,
                got,
            } => write!(
                f,
                "registry record_kind {record_kind:#04x} length prefix \
                 claimed {need} bytes, only {got} remain"
            ),
            RegistryError::ReservedIdRegistered { record_kind } => write!(
                f,
                "registry record_kind {record_kind:#04x} attempted to \
                 register reserved ID 0"
            ),
            RegistryError::AlreadyRegistered { namespace, id } => {
                write!(f, "{namespace} ID {id} was already registered (REG14)")
            }
            RegistryError::UnregisteredReference { namespace, id } => {
                write!(
                    f,
                    "{namespace} ID {id} was referenced before being \
                     registered (REG12)"
                )
            }
            RegistryError::ReservedIdTargeted { namespace } => write!(
                f,
                "NameAliased targeted the reserved {namespace} ID 0 (REG17)"
            ),
            RegistryError::NameAlreadyBound { namespace, name } => write!(
                f,
                "{namespace} name {name:?} is already bound to a different ID \
                 (REG16)"
            ),
            RegistryError::InvalidTargetKind(k) => {
                write!(f, "invalid NameAliased target_kind {k}")
            }
            RegistryError::NonZeroHighBits { target_id } => write!(
                f,
                "event_type alias target_id {target_id:#x} has nonzero high \
                 32 bits (D-REG-E)"
            ),
            RegistryError::ScopeIdNonZeroHighBits { scope_id } => write!(
                f,
                "DictRegistered event_type scope_id {scope_id:#x} has nonzero \
                 high 32 bits (D-REG-E)"
            ),
            RegistryError::ReservedCodecId { record_kind } => write!(
                f,
                "registry record_kind {record_kind:#04x} declared reserved \
                 codec_id 0"
            ),
            RegistryError::InvalidScopeKind(k) => {
                write!(f, "invalid DictRegistered scope_kind {k}")
            }
            RegistryError::Backend(e) => write!(f, "backend error: {e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for RegistryError<E> {}
