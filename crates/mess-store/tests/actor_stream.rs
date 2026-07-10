//! bn-2i3: the store's **authored command** path threads a command's stream
//! identity into the write and enforces it.
//!
//! A self-referential invariant (here, "a person greets *themselves*") forces
//! the command to carry the actor id its stream key already implies. That echo
//! makes a divergence — a `Greet { who: A }` dispatched to `person-B` —
//! representable, and `decide`, which never learns the stream id, cannot catch
//! it. [`EventStore::command_as`] closes the gap: it asks the command, via the
//! [`Actor`] trait, which stream it is authored against and refuses to write if
//! that disagrees with the stream it was dispatched to.
//!
//! These tests prove the mechanism against the in-memory backend:
//! - a matching actor commits and the event lands on its own stream, and
//! - a diverging actor is caught *at command time*, before any append.

use std::convert::Infallible;

use mess_core::{Actor, Aggregate, CodecError, Decide, Event};
// Only the release-build divergence test names this type; in a debug build
// (the `cargo test` default) the guard panics instead of returning it.
#[cfg(not(debug_assertions))]
use mess_store::AuthoredCommandError;
use mess_store::{EventStore, MockBackend, Version};

// ---------------------------------------------------------------------------
// A tiny person aggregate whose one command is self-referential: `Greet` names
// the person doing the greeting, and that person *is* the stream it belongs to.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum PersonEvent {
    Greeted { by: u64 },
}

impl Event for PersonEvent {
    fn name(&self) -> &'static str {
        match self {
            PersonEvent::Greeted { .. } => "person.greeted",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let PersonEvent::Greeted { by } = self;
        Ok(by.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let bytes: [u8; 8] =
            data.try_into().map_err(|_| CodecError::Decode {
                event_name: name.to_string(),
                source:     format!("expected 8 bytes, got {}", data.len()),
            })?;
        match name {
            "person.greeted" => {
                Ok(PersonEvent::Greeted { by: u64::from_le_bytes(bytes) })
            }
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Person {
    greetings: u32,
    last_by:   u64,
}

impl Aggregate for Person {
    type Event = PersonEvent;

    fn apply(&mut self, event: &PersonEvent) {
        match event {
            PersonEvent::Greeted { by } => {
                self.greetings += 1;
                self.last_by = *by;
            }
        }
    }
}

/// `who` is the person doing the greeting — the id of the stream this command
/// belongs to, restated here exactly as social's `Follow.follower` is.
#[derive(Debug, Clone, Copy)]
struct Greet {
    who: u64,
}

/// The single source of the `person-<id>` stream convention, used by both the
/// dispatcher below and the [`Actor`] impl — so the store compares like spelled
/// with like.
fn person_stream(id: u64) -> String { format!("person-{id}") }

impl Actor for Greet {
    fn actor_stream(&self) -> String { person_stream(self.who) }
}

impl Decide<Greet> for Person {
    type Rejection = Infallible;

    fn decide(&self, cmd: Greet) -> Result<Vec<PersonEvent>, Infallible> {
        Ok(vec![PersonEvent::Greeted { by: cmd.who }])
    }
}

fn store() -> EventStore<MockBackend> { EventStore::new(MockBackend::new()) }

// ---------------------------------------------------------------------------
// The matching actor: the command commits and the event lands on its own
// stream, carrying the very id the stream key implies.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn command_as_commits_when_actor_matches_stream() {
    let store = store();
    let alice = 7_u64;

    let commit = store
        .command_as::<Person, _>(&person_stream(alice), Greet { who: alice })
        .await
        .expect("a matching actor must commit");
    assert_eq!(commit.version, Version::At(0));
    assert_eq!(commit.events_appended, 1);

    // The event landed on alice's own stream, tagged with alice.
    let loaded = store.load::<Person>(&person_stream(alice)).await.unwrap();
    assert_eq!(loaded.state.greetings, 1);
    assert_eq!(loaded.state.last_by, alice);
    assert_eq!(loaded.version, Version::At(0));
}

// ---------------------------------------------------------------------------
// The diverging actor: `who = alice` dispatched to bob's stream. The store
// catches it at command time.
//
// The catch is a `debug_assert` — a loud, zero-release-cost failure in the
// dev/test builds where such a bug is born — so under `cfg(debug_assertions)`
// (the default for `cargo test`) the guard *panics*. In release the same
// divergence returns `AuthoredCommandError::ActorMismatch` instead; the
// release-only path is asserted separately below.
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg(debug_assertions)]
#[should_panic(expected = "actor/stream divergence")]
async fn command_as_debug_asserts_on_divergence() {
    let store = store();
    let alice = 7_u64;
    let bob = 9_u64;

    // `who = alice` but dispatched to bob's stream: the debug guard fires.
    let _ = store
        .command_as::<Person, _>(&person_stream(bob), Greet { who: alice })
        .await;
}

// ---------------------------------------------------------------------------
// In a release build (no `debug_assertions`) the same divergence is a
// fail-fast typed error carrying both stream ids, returned before any append —
// so no event is ever written to the wrong stream.
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg(not(debug_assertions))]
async fn command_as_errors_on_divergence_in_release() {
    let store = store();
    let alice = 7_u64;
    let bob = 9_u64;

    let err = store
        .command_as::<Person, _>(&person_stream(bob), Greet { who: alice })
        .await
        .expect_err("a diverging actor must be refused");
    match err {
        AuthoredCommandError::ActorMismatch { declared, stream } => {
            assert_eq!(declared, person_stream(alice));
            assert_eq!(stream, person_stream(bob));
        }
        other => panic!("expected ActorMismatch, got: {other}"),
    }

    // Nothing was written to bob's stream.
    let loaded = store.load::<Person>(&person_stream(bob)).await.unwrap();
    assert_eq!(loaded.version, Version::NoStream);
    assert_eq!(loaded.state.greetings, 0);
}
