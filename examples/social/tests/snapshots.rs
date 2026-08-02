//! Integration coverage for the four aggregates' `Snapshottable` impls and the
//! warm `command_cached` write path, exercised through the **real**
//! `EventStore` API (the same calls the app makes) over a snapshot-capable
//! `MockBackend`.
//!
//! Three properties, matching the store's own promises:
//!
//! 1. **Warm writes never change results (`cache-off == cache-miss`).** The
//!    same command sequence driven through `command` and through
//!    `command_cached` yields byte-identical folded state and identical typed
//!    rejections. This is the property the shared `WriteOps` blanket impl would
//!    inherit for free if the demo's backing store were snapshot-capable (see
//!    `contracts.rs`).
//! 2. **Snapshot-accelerated load == full replay.** `save_snapshot` then
//!    `load_cached` reconstructs the exact state `load` would, via snapshot +
//!    (empty) tail — the acceptance equivalence, per aggregate.
//! 3. **A stale `fold_version` is invalidated, not trusted.** A snapshot whose
//!    `fold_version` does not match the aggregate's is ignored and the state is
//!    rebuilt by full replay — even when its blob encodes a *different* state.

use mess_store::snapshot::{SnapshotStore, StoredSnapshot};
use mess_store::{
    CommandError, EventStore, MockBackend, SnapshotCompatibility,
    SnapshotCoverage, SnapshotRef, SnapshotTrust, Snapshottable,
    interim_stream_id,
};
use social::Id;
use social::domain::follow::Follow;
use social::domain::like::Like;
use social::domain::post::Post;
use social::domain::user::User;
use social::{
    CreatePost, DeletePost, LikeError, PlaceFollow, PlaceLike, RegisterUser,
    RemoveFollow, RemoveLike, SetDisplayName, follow_stream, like_stream,
    post_stream, user_stream,
};

/// A plain store (cache off) and a warm store (cache on), each over its **own**
/// fresh snapshot-capable backend, so the same sequence run on both is a clean
/// differential.
fn plain_and_warm() -> (EventStore<MockBackend>, EventStore<MockBackend>) {
    let plain = EventStore::new(MockBackend::new());
    let warm = EventStore::new(MockBackend::new()).with_cache_capacity(64);
    (plain, warm)
}

#[tokio::test]
async fn like_warm_path_matches_cold_path() {
    let (plain, warm) = plain_and_warm();
    let post = Id::new();
    let user = Id::new();
    let s = like_stream(post, user);

    // A like / unlike / re-like sequence, plus a double-like that must reject
    // identically on both paths.
    for store_is_warm in [false, true] {
        let (store, other) =
            if store_is_warm { (&warm, "warm") } else { (&plain, "cold") };
        let _ = other;

        macro_rules! place {
            () => {
                if store_is_warm {
                    store.command_cached::<Like, _>(&s, PlaceLike).await
                } else {
                    store.command::<Like, _>(&s, PlaceLike).await
                }
            };
        }
        macro_rules! remove {
            () => {
                if store_is_warm {
                    store.command_cached::<Like, _>(&s, RemoveLike).await
                } else {
                    store.command::<Like, _>(&s, RemoveLike).await
                }
            };
        }

        place!().expect("first like");
        // Double like: same typed rejection on both paths.
        match place!() {
            Err(CommandError::Domain(LikeError::AlreadyLiked)) => {}
            other => panic!("expected AlreadyLiked, got {other:?}"),
        }
        remove!().expect("unlike");
        match remove!() {
            Err(CommandError::Domain(LikeError::NotLiked)) => {}
            other => panic!("expected NotLiked, got {other:?}"),
        }
        place!().expect("re-like");
    }

    let cold = plain.load::<Like>(&s).await.unwrap().state;
    let hot = warm.load::<Like>(&s).await.unwrap().state;
    assert_eq!(cold, hot, "warm and cold folds must be identical");
    assert!(hot.liked, "final state is liked on both paths");
}

#[tokio::test]
async fn user_warm_path_matches_cold_path() {
    let (plain, warm) = plain_and_warm();
    let id = Id::new();
    let s = user_stream(id);

    for (store, warm_path) in [(&plain, false), (&warm, true)] {
        if warm_path {
            store
                .command_cached::<User, _>(
                    &s,
                    RegisterUser {
                        handle:       "alice".into(),
                        display_name: "Alice".into(),
                    },
                )
                .await
                .unwrap();
            store
                .command_cached::<User, _>(
                    &s,
                    SetDisplayName { display_name: "Alice 🎉".into() },
                )
                .await
                .unwrap();
        } else {
            store
                .command::<User, _>(
                    &s,
                    RegisterUser {
                        handle:       "alice".into(),
                        display_name: "Alice".into(),
                    },
                )
                .await
                .unwrap();
            store
                .command::<User, _>(
                    &s,
                    SetDisplayName { display_name: "Alice 🎉".into() },
                )
                .await
                .unwrap();
        }
    }

    let cold = plain.load::<User>(&s).await.unwrap();
    let hot = warm.load::<User>(&s).await.unwrap();
    assert_eq!(cold.state, hot.state);
    assert_eq!(cold.version, hot.version);
    assert_eq!(hot.state.display_name, "Alice 🎉");
}

/// `save_snapshot` then `load_cached` must reconstruct exactly what `load`
/// (full replay) yields — for every aggregate, exercising each state codec end
/// to end through the store.
#[tokio::test]
async fn snapshot_then_load_cached_equals_full_replay() {
    let store = EventStore::new(MockBackend::new());
    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    // Seed one stream of each family via plain commands.
    let us = user_stream(alice);
    store
        .command::<User, _>(
            &us,
            RegisterUser {
                handle:       "alice".into(),
                display_name: "A".into(),
            },
        )
        .await
        .unwrap();
    store
        .command::<User, _>(
            &us,
            SetDisplayName { display_name: "Alice".into() },
        )
        .await
        .unwrap();

    let ps = post_stream(post);
    store
        .command::<Post, _>(
            &ps,
            CreatePost { author: bob, body: "hello".into() },
        )
        .await
        .unwrap();
    store.command::<Post, _>(&ps, DeletePost { by: bob }).await.unwrap();

    let ls = like_stream(post, alice);
    store.command::<Like, _>(&ls, PlaceLike).await.unwrap();

    let fs = follow_stream(alice, bob);
    store.command::<Follow, _>(&fs, PlaceFollow).await.unwrap();
    store.command::<Follow, _>(&fs, RemoveFollow).await.unwrap();

    // For each stream: full replay, snapshot, snapshot-accelerated load, assert
    // equal and that the tail after a head snapshot is empty.
    macro_rules! check {
        ($agg:ty, $stream:expr) => {{
            let full = store.load::<$agg>($stream).await.unwrap();
            store.save_snapshot::<$agg>($stream).await.unwrap();
            let via_snap = store.load_cached::<$agg>($stream).await.unwrap();
            assert_eq!(
                full.state,
                via_snap.state,
                "snapshot load must equal full replay for {}",
                stringify!($agg)
            );
            assert_eq!(
                via_snap.events_replayed,
                0,
                "a head snapshot leaves an empty tail for {}",
                stringify!($agg)
            );
        }};
    }
    check!(User, &us);
    check!(Post, &ps);
    check!(Like, &ls);
    check!(Follow, &fs);
}

/// A snapshot whose `fold_version` does not match the aggregate's must be
/// ignored: the load rebuilds by full replay and returns the correct state,
/// even though the stale blob encodes a deliberately *wrong* one.
#[tokio::test]
async fn stale_fold_version_is_invalidated_not_trusted() {
    let store = EventStore::new(MockBackend::new());
    let id = Id::new();
    let s = user_stream(id);

    store
        .command::<User, _>(
            &s,
            RegisterUser {
                handle:       "realhandle".into(),
                display_name: "Real".into(),
            },
        )
        .await
        .unwrap();
    let correct = store.load::<User>(&s).await.unwrap().state;

    // Plant a snapshot under a DIFFERENT identity whose blob decodes to a
    // completely different user. `load_cached` must never fall back across
    // identities, however plausible the bytes look.
    let bogus = User {
        registered:   true,
        handle:       "wronghandle".into(),
        display_name: "WRONG".into(),
    };
    let planted = StoredSnapshot {
        snapshot_ref: SnapshotRef {
            compatibility: SnapshotCompatibility {
                fold_version: User::FOLD_VERSION.wrapping_add(9999),
                ..User::snapshot_compatibility()
            },
            coverage:      SnapshotCoverage::Through(0),
            trust:         SnapshotTrust::UnverifiedCache,
            stream_id:     interim_stream_id(&s),
        },
        state_blob:   bogus.encode_state().unwrap(),
    };
    store.backend().save_snapshot(&s, planted).await.unwrap();

    // load_cached must ignore the stale snapshot and full-replay to the truth.
    let loaded = store.load_cached::<User>(&s).await.unwrap();
    assert_eq!(loaded.state, correct);
    assert_eq!(loaded.state.handle, "realhandle");
    assert_ne!(loaded.state.handle, "wronghandle");
}
