//! bn-5iu: `Durability` is reachable through `mess-store`'s public API alone.
//!
//! The dogfood finding (bn-o9z): `examples/social`'s seeder could not
//! demonstrate `Durability::Group` (fsync-coalesced group commit, the bulk
//! writes guide's headline speedup, `docs/perf/bulk-writes.md`) because
//! selecting it required a direct `mess-log` dependency, which an app
//! example correctly refuses. This file proves the fix end to end: open a
//! [`LogEngine`] under `Durability::Group` using only the `mess_store::
//! Durability` re-export (no `mess_log` import here), and commit a few
//! events through it.
#![cfg(not(miri))]

use std::time::Duration;

use mess_store::backend::{Backend, RecordToAppend};
// The load-bearing import: `Durability` reached through `mess-store`'s
// crate root, not `mess_log::committer::Durability`.
use mess_store::{Durability, EngineOptions, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

#[tokio::test]
async fn group_durability_selected_via_reexport_commits_events() {
    let dir = tempfile::tempdir().unwrap();
    let opts = EngineOptions {
        durability: Durability::Group {
            max_delay: Duration::from_millis(1),
            max_bytes: 8 * 1024 * 1024,
        },
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(dir.path(), opts).expect("open");

    let a = engine
        .append_batch(
            "acct-1",
            Version::NoStream,
            &[rec("Opened", b"x"), rec("Deposited", b"5")],
        )
        .await
        .expect("append under Durability::Group");
    assert_eq!(a.version, Version::At(1));
    assert_eq!(a.last_global_position, 1);

    let b = engine
        .append_batch("acct-1", Version::At(1), &[rec("Withdrew", b"2")])
        .await
        .expect("second append under Durability::Group");
    assert_eq!(b.version, Version::At(2));

    let page =
        engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(page.len(), 3);
    assert_eq!(page[2].message_type, "Withdrew");
    assert_eq!(page[2].data, b"2");
}

/// `Durability::group_default()` (the spec's recommended `1ms`/`8MiB`
/// defaults) is also reachable through the re-export.
#[tokio::test]
async fn group_default_via_reexport_opens_and_commits() {
    let dir = tempfile::tempdir().unwrap();
    let opts = EngineOptions {
        durability: Durability::group_default(),
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(dir.path(), opts).expect("open");

    engine
        .append_batch("s", Version::NoStream, &[rec("A", b"1")])
        .await
        .expect("append under group_default");
    assert_eq!(engine.head("s").await.unwrap(), Version::At(0));
}
