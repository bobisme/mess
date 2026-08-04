//! The **User** aggregate: identity and display name — one **shallow** stream
//! per user, keyed `user-<id>` (see [`crate::user_stream`]).
//!
//! This half of the corpus exists for **registry pressure**: `--users 50000`
//! means fifty thousand distinct stream ids the engine must register, intern,
//! and carry in its per-segment registry deltas, while each stream holds one or
//! two events. It is deliberately the same shape `examples/social` produces
//! everywhere — and on its own it is exactly the shape bn-3jqg found
//! insufficient (1.007 events/stream, no segment ever rolls). Chatter pairs it
//! with [`super::channel`], whose streams are thousands of events deep, so a
//! single corpus carries *both* pressures.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{Snapshottable, StableSnapshotId, StateCodecError};

use crate::domain::snapshot_codec;
use crate::domain::snapshot_codec::{Reader, put_bool, put_str};

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Every fact that can happen to a user.
///
/// `#[event(name = "user", version = 1)]` gives `Registered` the stored wire
/// name `"user.registered"` and `DisplayNameChanged` →
/// `"user.display_name_changed"`; the derive keys decode on that stored *name
/// string*, so reordering variants never silently re-tags stored bytes.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "user", version = 1)]
pub enum UserEvent {
    /// The user joined: their immutable `handle` and initial `display_name`.
    Registered { handle: String, display_name: String },
    /// The user changed their (mutable) display name.
    DisplayNameChanged { display_name: String },
}

// ---------------------------------------------------------------------------
// The aggregate
// ---------------------------------------------------------------------------

/// The read-model folded from one user's event stream. **Bounded**: an
/// existence flag and two short strings.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = UserEvent)]
pub struct User {
    /// `false` until a `Registered` event is folded.
    pub registered:   bool,
    /// The immutable handle chosen at registration.
    pub handle:       String,
    /// The current display name.
    pub display_name: String,
}

impl User {
    /// Fold one event into state. Infallible by construction: anything in the
    /// log was already proved legal by a prior `decide`.
    pub fn apply(&mut self, event: &UserEvent) {
        match event {
            UserEvent::Registered { handle, display_name } => {
                self.registered = true;
                handle.clone_into(&mut self.handle);
                display_name.clone_into(&mut self.display_name);
            }
            UserEvent::DisplayNameChanged { display_name } => {
                display_name.clone_into(&mut self.display_name);
            }
        }
    }
}

/// A [`User`] snapshot is an existence bit plus two short, length-capped
/// strings.
///
/// **`FOLD_VERSION` bump rule.** Bump whenever [`User::apply`] semantics or the
/// blob shape change such that an old blob would misrepresent the state.
impl Snapshottable for User {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("chatter.user");
    const CODEC_ID: StableSnapshotId = snapshot_codec::CODEC_ID;
    const CODEC_VERSION: u32 = snapshot_codec::CODEC_VERSION;
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::new();
        put_bool(&mut out, self.registered);
        put_str(&mut out, &self.handle);
        put_str(&mut out, &self.display_name);
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let mut r = Reader::new(bytes);
        let registered = r.read_bool()?;
        let handle = r.read_str()?;
        let display_name = r.read_str()?;
        r.finish()?;
        Ok(User { registered, handle, display_name })
    }
}

// ---------------------------------------------------------------------------
// Handle validation
// ---------------------------------------------------------------------------

/// The maximum length, in characters, of a user handle.
pub const HANDLE_MAX_LEN: usize = 30;

/// Is `handle` a syntactically valid user handle? 1 to [`HANDLE_MAX_LEN`]
/// characters of ASCII lowercase, digits, or `_`.
#[must_use]
pub fn handle_is_valid(handle: &str) -> bool {
    let len = handle.chars().count();
    (1..=HANDLE_MAX_LEN).contains(&len)
        && handle
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Every way a command against [`User`] can be refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserError {
    /// `RegisterUser` on a stream that already has a registered user.
    AlreadyRegistered,
    /// Any command other than `RegisterUser` on an unregistered stream.
    NotRegistered,
    /// `RegisterUser` with a handle that fails [`handle_is_valid`].
    InvalidHandle { handle: String },
}

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserError::AlreadyRegistered => {
                write!(f, "user is already registered")
            }
            UserError::NotRegistered => write!(f, "user is not registered"),
            UserError::InvalidHandle { handle } => write!(
                f,
                "invalid handle {handle:?}: must be 1-{HANDLE_MAX_LEN} \
                 characters of lowercase a-z, 0-9, or underscore"
            ),
        }
    }
}

impl std::error::Error for UserError {}

/// Register a new user with an immutable `handle` and an initial
/// `display_name`.
#[derive(Debug, Clone)]
pub struct RegisterUser {
    pub handle:       String,
    pub display_name: String,
}

/// Change the (mutable) display name.
#[derive(Debug, Clone)]
pub struct SetDisplayName {
    pub display_name: String,
}

impl Decide<RegisterUser> for User {
    type Rejection = UserError;

    fn decide(&self, cmd: RegisterUser) -> Result<Vec<UserEvent>, UserError> {
        if self.registered {
            return Err(UserError::AlreadyRegistered);
        }
        if !handle_is_valid(&cmd.handle) {
            return Err(UserError::InvalidHandle { handle: cmd.handle });
        }
        Ok(vec![UserEvent::Registered {
            handle:       cmd.handle,
            display_name: cmd.display_name,
        }])
    }
}

impl Decide<SetDisplayName> for User {
    type Rejection = UserError;

    fn decide(&self, cmd: SetDisplayName) -> Result<Vec<UserEvent>, UserError> {
        if !self.registered {
            return Err(UserError::NotRegistered);
        }
        Ok(vec![UserEvent::DisplayNameChanged {
            display_name: cmd.display_name,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_through_bytes() {
        let states = [
            User::default(),
            User {
                registered:   true,
                handle:       "quiet_otter12".into(),
                display_name: "Priya Okafor 🎉".into(),
            },
        ];
        for state in states {
            let bytes = state.encode_state().unwrap();
            assert_eq!(User::decode_state(&bytes).unwrap(), state);
        }
    }

    #[test]
    fn malformed_blob_errors_not_panics() {
        assert!(User::decode_state(&[1]).is_err());
        assert!(User::decode_state(&[]).is_err());
    }

    #[test]
    fn register_rejects_an_invalid_handle() {
        let u = User::default();
        assert!(matches!(
            u.decide(RegisterUser {
                handle:       "NotLower".into(),
                display_name: "x".into(),
            }),
            Err(UserError::InvalidHandle { .. })
        ));
    }

    #[test]
    fn double_register_is_refused() {
        let mut u = User::default();
        u.apply(&UserEvent::Registered {
            handle:       "a".into(),
            display_name: "A".into(),
        });
        assert_eq!(
            u.decide(RegisterUser {
                handle:       "b".into(),
                display_name: "B".into(),
            }),
            Err(UserError::AlreadyRegistered)
        );
    }
}
