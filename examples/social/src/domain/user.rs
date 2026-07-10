//! The **User** aggregate: identity, display name, and the follow graph.
//!
//! One stream per user, keyed `user-<id>` (see [`crate::user_stream`]). Every
//! rule below is enforced in [`Decide::decide`] against a single stream's
//! folded state — the whole point of an aggregate boundary. See
//! `examples/bank/src/lib.rs` for the first walkthrough of
//! `#[derive(Event)]` / `#[derive(Aggregate)]` / `Decide`; this module adds
//! two things bank did not have: input **validation** (handle syntax) and a
//! **set-valued invariant** (the follow set).
//!
//! # Why the follow SET lives in the *follower's* own aggregate
//!
//! A follow edge `alice -> bob` is recorded as a `Followed { target: bob }`
//! event on **alice's** stream, and alice's folded state carries the set of
//! ids she currently follows. It is deliberately *not* stored on bob's
//! stream, nor in some third "edges" aggregate. Why:
//!
//! - **Single-stream invariant enforcement.** The rules Follow must uphold —
//!   *not already following*, *not self*, *registered* — are all facts about
//!   **alice**. Because `decide` folds exactly one stream, every fact it needs
//!   to check must live on that one stream. Put the set on alice's stream and
//!   "am I already following bob?" is a pure, race-free lookup in alice's own
//!   folded state. The optimistic-concurrency retry in `EventStore::command`
//!   then makes the check-and-append atomic *per stream* with no cross-stream
//!   lock.
//!
//! - **Cross-aggregate existence checks are deliberately impossible in
//!   `decide`.** Notice what Follow does **not** verify: that `bob` exists and
//!   is registered. `decide` is a pure function of *one* aggregate's state; it
//!   cannot load bob's stream. That is a feature, not a gap. The event-sourcing
//!   answer to "does the target exist?" is: don't enforce it synchronously in
//!   the writer. Either (a) accept the edge and let a downstream
//!   projection/read-model reconcile or drop dangling edges, or (b) enforce it
//!   in a process manager / saga that reacts to `Followed` and emits a
//!   compensating `Unfollowed` if the target turns out not to exist. A single
//!   `decide` call spanning two streams would need a distributed transaction —
//!   exactly what event sourcing trades away for per-stream linearizability.
//!   (Logged as a dogfood finding for bn-154.)

use std::collections::HashSet;

use ident::Id;
use mess_core::{Actor, Decide};
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one user's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a user.
///
/// `#[event(name = "user", version = 1)]` gives `Registered` the stored wire
/// name `"user.registered"`, `Followed` → `"user.followed"`, and so on — the
/// derive keys decode on that stored *name string*, so reordering variants
/// never silently re-tags stored bytes (see `examples/bank`).
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "user", version = 1)]
pub enum UserEvent {
    /// The user joined: their immutable `handle` and initial `display_name`.
    Registered { handle: String, display_name: String },
    /// The user changed their (mutable) display name.
    DisplayNameChanged { display_name: String },
    /// The user started following `target`.
    Followed { target: Id },
    /// The user stopped following `target`.
    Unfollowed { target: Id },
}

// ---------------------------------------------------------------------------
// The aggregate: one user's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one user's event stream.
///
/// `following` is a [`HashSet`], and `HashSet` equality is order-independent,
/// so the derived `PartialEq`/`Eq` on `User` stays a true value comparison
/// regardless of insertion order. (Dogfood finding for bn-154: the natural
/// first choice was a `BTreeSet` for order-stable folding, but `ident::Id`
/// does not implement `Ord`, so it cannot go in a `BTreeSet` — only `Hash`.
/// A future fold-certificate that hashes aggregate state will therefore need
/// an order-independent set hash, or `Id` will need an `Ord` impl.)
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
    /// The set of user ids this user currently follows — the follow edge set,
    /// living on the follower's own stream (see the module docs).
    pub following:    HashSet<Id>,
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
            UserEvent::Followed { target } => {
                self.following.insert(*target);
            }
            UserEvent::Unfollowed { target } => {
                self.following.remove(target);
            }
        }
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
    /// `Follow` where the follower and target are the same id.
    SelfFollow,
    /// `Follow` on a target already in the follow set.
    AlreadyFollowing,
    /// `Unfollow` on a target not in the follow set.
    NotFollowing,
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
            UserError::SelfFollow => {
                write!(f, "a user cannot follow themselves")
            }
            UserError::AlreadyFollowing => {
                write!(f, "already following that user")
            }
            UserError::NotFollowing => write!(f, "not following that user"),
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

/// Follow another user.
///
/// `follower` is the id of the user *this stream belongs to*. It is restated
/// in the command because [`Decide::decide`] sees only the folded aggregate
/// state, never the stream id it was loaded from — so the self-follow check has
/// no other way to learn "who am I?". A self-referential invariant forces the
/// command to echo the actor id that the stream key `user-<follower>` already
/// implies.
///
/// # The echoed actor id is now *checked*, not merely trusted (bn-2i3)
///
/// Echoing the id makes a divergence **representable**: a
/// `Follow { follower: X }` committed to *Y*'s stream type-checks, and `decide`
/// — which never learns Y — cannot catch it. The blessed remedy is the
/// [`Actor`] convention: `Follow` declares, via [`Actor::actor_stream`], the
/// stream it belongs to (built from `follower` through the one
/// [`user_stream`](crate::user_stream) helper the writer also dispatches with),
/// and the store's authored command path
/// ([`command_as`](mess_store::EventStore::command_as)) asserts the two agree
/// **before** deciding. The field stays — the self-follow rule still needs it —
/// but a `follower`/stream divergence is now a fail-fast
/// [`AuthoredCommandError::ActorMismatch`](mess_store::AuthoredCommandError),
/// not a silent mis-write.
#[derive(Debug, Clone, Copy)]
pub struct Follow {
    pub follower: Id,
    pub target:   Id,
}

impl Actor for Follow {
    /// A `Follow` is authored against the *follower's* own stream — the same
    /// `user-<follower>` key [`WriteOps`](crate::WriteOps) dispatches it to.
    /// Reusing [`user_stream`](crate::user_stream) keeps the two spellings a
    /// single source of truth, so the store's dispatch check compares like with
    /// like.
    fn actor_stream(&self) -> String { crate::user_stream(self.follower) }
}

/// Unfollow a user. No `follower` field is needed: you can never be following
/// yourself (Follow forbids it), so a self-target simply lands on
/// [`UserError::NotFollowing`], and every other rule is a lookup in this
/// stream's own follow set.
#[derive(Debug, Clone, Copy)]
pub struct Unfollow {
    pub target: Id,
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

impl Decide<Follow> for User {
    type Rejection = UserError;

    fn decide(&self, cmd: Follow) -> Result<Vec<UserEvent>, UserError> {
        if !self.registered {
            return Err(UserError::NotRegistered);
        }
        if cmd.follower == cmd.target {
            return Err(UserError::SelfFollow);
        }
        if self.following.contains(&cmd.target) {
            return Err(UserError::AlreadyFollowing);
        }
        Ok(vec![UserEvent::Followed { target: cmd.target }])
    }
}

impl Decide<Unfollow> for User {
    type Rejection = UserError;

    fn decide(&self, cmd: Unfollow) -> Result<Vec<UserEvent>, UserError> {
        if !self.registered {
            return Err(UserError::NotRegistered);
        }
        if !self.following.contains(&cmd.target) {
            return Err(UserError::NotFollowing);
        }
        Ok(vec![UserEvent::Unfollowed { target: cmd.target }])
    }
}
