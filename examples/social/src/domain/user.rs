//! The **User** aggregate: identity and display name.
//!
//! One stream per user, keyed `user-<id>` (see [`crate::user_stream`]). Every
//! rule below is enforced in [`Decide::decide`] against a single stream's
//! folded state — the whole point of an aggregate boundary. See
//! `examples/bank/src/lib.rs` for the first walkthrough of
//! `#[derive(Event)]` / `#[derive(Aggregate)]` / `Decide`; this module adds
//! input **validation** (handle syntax) that bank did not have.
//!
//! # Bounded state: the follow *graph* is not stored here
//!
//! An earlier version of this crate folded a `following: HashSet<Id>` — the
//! set of everyone this user follows — into the user's own state, with
//! `Followed` / `Unfollowed` events on this stream. That is **unbounded**
//! aggregate state: a user following ten thousand accounts meant a
//! ten-thousand-element set replayed on every `User` command and written into
//! every snapshot. This rework moves each follow edge to its own tiny
//! [`Follow`](super::follow::Follow) relationship stream
//! (`follow-<follower>_<followee>`), so `User` keeps only **bounded** state —
//! registration and display name — and its commands stay O(1) regardless of
//! how large the follow graph grows. The `AlreadyFollowing` / `NotFollowing`
//! rejections moved with the relationship; the `SelfFollow` check moved to the
//! [`WriteOps`](crate::WriteOps) seam (see [`super::follow`]).
//!
//! # The accept-and-reconcile posture (unchanged, and now the *only* place it
//! is documented)
//!
//! This module has always carried the crate's canonical statement of *why a
//! `decide` deliberately cannot make cross-aggregate existence checks* — the
//! posture the project's official docs (bn-2v0) link to. Moving the follow
//! edge out of `User` does not change that lesson one bit; it sharpens it, so
//! the statement stays here, restated for the relationship shape:
//!
//! - **Single-stream invariant enforcement.** Every rule a command upholds must
//!   be a fact about the *one* stream `decide` folds. The old `Follow` checked
//!   "am I already following bob?" against alice's own follow set; the new
//!   [`Follow`](super::follow::Follow) checks "is this one edge already
//!   active?" against the `follow-<alice>_<bob>` stream's own folded [`bool`].
//!   Both are pure, race-free lookups in a single stream's state, made atomic
//!   per stream by the optimistic-concurrency retry in
//!   [`EventStore::command`](mess_store::EventStore::command) — with no
//!   cross-stream lock.
//!
//! - **Cross-aggregate existence checks are deliberately impossible in
//!   `decide`.** Notice what following still does **not** verify: that the
//!   followee `bob` exists and is registered. `decide` is a pure function of
//!   *one* aggregate's state; it cannot load bob's `user-<bob>` stream. That is
//!   a feature, not a gap. The event-sourcing answer to "does the target
//!   exist?" is: don't enforce it synchronously in the writer. Either (a)
//!   accept the edge and let a downstream projection/read-model reconcile or
//!   drop dangling edges, or (b) enforce it in a process manager / saga that
//!   reacts to the follow and emits a compensating unfollow if the target turns
//!   out not to exist. A single `decide` spanning two streams would need a
//!   distributed transaction — exactly what event sourcing trades away for
//!   per-stream linearizability. The projection ([`crate::projections`]) is
//!   where the reconciliation happens: it filters home timelines by the
//!   viewer's *current* follow set at query time, so a dangling or retracted
//!   edge simply contributes nothing.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{Snapshottable, StableSnapshotId, StateCodecError};

use crate::domain::snapshot_codec;
use crate::domain::snapshot_codec::{Reader, put_bool, put_str};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one user's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a user.
///
/// `#[event(name = "user", version = 1)]` gives `Registered` the stored wire
/// name `"user.registered"` and `DisplayNameChanged` → `"user.display_name_
/// changed"` — the derive keys decode on that stored *name string*, so
/// reordering variants never silently re-tags stored bytes (see
/// `examples/bank`). (Follows are no longer user events — they live on the
/// `follow-<follower>_<followee>` streams; see the module docs.)
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "user", version = 1)]
pub enum UserEvent {
    /// The user joined: their immutable `handle` and initial `display_name`.
    Registered { handle: String, display_name: String },
    /// The user changed their (mutable) display name.
    DisplayNameChanged { display_name: String },
}

// ---------------------------------------------------------------------------
// The aggregate: one user's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one user's event stream — now fully **bounded**:
/// an existence flag and two strings, no follow set.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = UserEvent)]
pub struct User {
    /// `false` until a `Registered` event is folded — the existence flag every
    /// command except `RegisterUser` checks first.
    pub registered:   bool,
    /// The immutable handle chosen at registration.
    pub handle:       String,
    /// The current display name (mutated by `DisplayNameChanged`).
    pub display_name: String,
}

impl User {
    /// Fold one event into state. Infallible by construction: anything in the
    /// log was already proved legal by a prior `decide` (see `examples/bank`).
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
/// strings — bounded by construction now that the follow graph lives on its own
/// relationship streams (`bn-jes`). No O(following) set is ever serialized.
///
/// **`FOLD_VERSION` bump rule.** Bump whenever [`User::apply`] semantics or the
/// `encode_state` / `decode_state` byte shape change such that a blob written
/// by the old fold would misrepresent the state (a newly-folded [`UserEvent`]
/// variant, a new/removed field, a reordered blob). A bump invalidates older
/// snapshots, which `load_cached` rebuilds by full replay (§9); a pure refactor
/// that preserves the folded state and blob shape does not bump.
impl Snapshottable for User {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("social.user");
    // The state codec is shared with the other three aggregates, so it is
    // named and versioned separately from this fold.
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
// Handle validation.
// ---------------------------------------------------------------------------

/// The maximum length, in characters, of a user handle.
pub const HANDLE_MAX_LEN: usize = 30;

/// Is `handle` a syntactically valid user handle?
///
/// The rule, documented once here so both `decide` and callers agree: a handle
/// is **1 to 30 characters**, each of which is an ASCII **lowercase letter**
/// (`a`–`z`), an ASCII **digit** (`0`–`9`), or an **underscore** (`_`). No
/// uppercase, no spaces, no Unicode. Length is counted in `char`s, which for
/// this ASCII-only alphabet equals bytes — but we count `char`s so the rule
/// still reads correctly if the alphabet is ever widened.
#[must_use]
pub fn handle_is_valid(handle: &str) -> bool {
    let len = handle.chars().count();
    (1..=HANDLE_MAX_LEN).contains(&len)
        && handle
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

// ---------------------------------------------------------------------------
// Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`User`] can be refused. A typed
/// [`Decide::Rejection`] callers match on exhaustively — never a string (see
/// `examples/bank`'s `AccountError`).
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
/// `display_name`. Commands are plain structs — never stored, never on the
/// wire; only the events `decide` returns cross into the log.
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
mod snapshot_tests {
    use super::*;

    #[test]
    fn state_round_trips_through_bytes() {
        let states = [
            User::default(),
            User {
                registered:   true,
                handle:       "alice".into(),
                display_name: "Alice 🎉".into(),
            },
            User {
                registered:   true,
                handle:       "under_score_30_chars_0000000_x".into(),
                display_name: String::new(),
            },
        ];
        for state in states {
            let bytes = state.encode_state().unwrap();
            assert_eq!(User::decode_state(&bytes).unwrap(), state);
        }
    }

    #[test]
    fn malformed_blob_errors_not_panics() {
        // Truncated: a registered flag but no handle length/bytes.
        assert!(User::decode_state(&[1]).is_err());
        // Empty blob.
        assert!(User::decode_state(&[]).is_err());
    }
}
