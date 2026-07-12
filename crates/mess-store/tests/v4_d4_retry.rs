//! bn-9mw Spike E, review D4: the two mandatory registration-retry crash tests.
//!
//! Scenario: a client's `register + append` capsule is made **durable but the
//! ack is lost** (crash after the barrier, before the client hears success).
//! On restart the engine recovers that capsule. The client then retries the
//! same `register + append`:
//!
//! - **(a) matching dedupe key** → the engine returns the ORIGINAL commit and
//!   appends NOTHING new (idempotent).
//! - **(b) no dedupe key** → the engine returns an expected-version conflict
//!   and appends NOTHING (the recovered stream head already advanced).
//!
//! The capsule/recovery semantics are what's under test; the engine here is a
//! deliberately small shim over the v4 [`CapsuleWriter`] / recovery and the
//! **real** [`RegistryState`] (review V6/S4: the seam folds controls into
//! `RegistryState`, never weakening `AlreadyRegistered`). No production engine
//! path is involved — v4 write is off by default.

use std::collections::HashMap;
use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::runtime::{FileHandle, Fs, OpenOpts, Runtime, SimRuntime};
use mess_log::v4::capsule::{CapsuleEncoder, CapsuleInput};
use mess_log::v4::control::ControlRecord;
use mess_log::v4::recover::{RegistryView, scan_v4_image};
use mess_log::v4::writer::{CapsuleWriter, SegmentParamsV4};
use mess_store::registry::{RegistryError, RegistryRecord, RegistryState};

// ---------------------------------------------------------------------------
// The RegistryView seam's PRODUCTION-shaped impl: fold decoded v4 controls into
// the real RegistryState (review V6/S4). Registration controls map 1:1 to
// RegistryRecords; dedupe/checkpoint/snapshot controls are not registry
// records and are ignored by this view (the engine handles dedupe separately).
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RegistryStateView {
    state: RegistryState,
}

impl RegistryView for RegistryStateView {
    type Reject = RegistryError;

    fn apply(&mut self, control: &ControlRecord) -> Result<(), RegistryError> {
        let rec = match control {
            ControlRecord::CategoryRegistered { category_id, name } => {
                Some(RegistryRecord::CategoryRegistered {
                    category_id: *category_id,
                    name:        name.clone(),
                })
            }
            ControlRecord::StreamRegistered {
                stream_id,
                category_id,
                name,
            } => Some(RegistryRecord::StreamRegistered {
                stream_id:   *stream_id,
                category_id: *category_id,
                name:        name.clone(),
            }),
            ControlRecord::EventTypeRegistered {
                event_type_id,
                codec_id,
                schema_fingerprint,
                name,
                ..
            } => Some(RegistryRecord::EventTypeRegistered {
                event_type_id:      *event_type_id,
                codec_id:           *codec_id,
                schema_fingerprint: *schema_fingerprint,
                name:               name.clone(),
            }),
            // Not registry records.
            ControlRecord::DedupeKey { .. }
            | ControlRecord::SnapshotInstalled { .. }
            | ControlRecord::ProjectionCheckpoint { .. } => None,
        };
        match rec {
            Some(r) => self.state.apply::<std::convert::Infallible>(r),
            None => Ok(()),
        }
    }

    fn event_type_resolves(&self, event_type_id: u32) -> bool {
        event_type_id == 0
            || self.state.event_type_name(event_type_id).is_some()
    }
}

// ---------------------------------------------------------------------------
// A minimal engine shim: the v4 capsule log + recovered registry + dedupe
// window + stream heads.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Commit {
    batch_id:         u64,
    first_global_pos: u64,
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Committed(Commit),
    /// The dedupe key was already committed — the original commit, nothing new.
    Idempotent(Commit),
    /// The recovered stream head already advanced past `expected`.
    Conflict {
        expected: u64,
        actual:   u64,
    },
}

struct Shim<'f, F: Fs> {
    fs:            &'f F,
    path:          &'f Path,
    epoch:         u64,
    write_off:     u64,
    next_batch_id: u64,
    next_gp:       u64,
    heads:         HashMap<u64, u64>, // stream_id -> next version (count)
    dedupe:        HashMap<Vec<u8>, Commit>,
    registry:      RegistryStateView,
    encoder:       CapsuleEncoder,
}

impl<'f, F: Fs> Shim<'f, F> {
    /// Create a fresh v4 segment.
    fn create(fs: &'f F, path: &'f Path, epoch: u64) -> Self {
        CapsuleWriter::create(fs, path, SegmentParamsV4::new(1, 0, epoch, 0))
            .unwrap()
            .close()
            .unwrap();
        let write_off = read_all(fs, path).len() as u64; // == SEGMENT_HEADER_LEN
        Shim {
            fs,
            path,
            epoch,
            write_off,
            next_batch_id: 0,
            next_gp: 0,
            heads: HashMap::new(),
            dedupe: HashMap::new(),
            registry: RegistryStateView::default(),
            encoder: CapsuleEncoder::new(),
        }
    }

    /// Recover an existing segment: replay accepted capsules into the registry,
    /// stream heads, and dedupe window (the restart-after-crash path).
    fn recover(fs: &'f F, path: &'f Path, epoch: u64) -> Self {
        let img = read_all(fs, path);
        let (rec, view) = scan_v4_image(&img, RegistryStateView::default());
        let mut heads: HashMap<u64, u64> = HashMap::new();
        let mut dedupe: HashMap<Vec<u8>, Commit> = HashMap::new();
        for ac in &rec.accepted {
            if ac.event_count > 0 {
                let head = ac.first_stream_version + u64::from(ac.event_count);
                let e = heads.entry(ac.stream_id).or_insert(0);
                *e = (*e).max(head);
            }
            // Recover dedupe keys carried by this capsule.
            let capsule =
                &img[ac.offset as usize..(ac.offset + ac.total_len) as usize];
            let decoded =
                mess_log::v4::capsule::decode_capsule(capsule, 0).unwrap();
            for ctl in &decoded.controls {
                if let ControlRecord::DedupeKey { key, .. } = ctl {
                    dedupe.insert(
                        key.clone(),
                        Commit {
                            batch_id:         ac.batch_id,
                            first_global_pos: ac.first_global_pos,
                        },
                    );
                }
            }
        }
        Shim {
            fs,
            path,
            epoch,
            write_off: rec.safe_offset,
            next_batch_id: rec.next_batch_id,
            next_gp: rec.next_global_pos,
            heads,
            dedupe,
            registry: view,
            encoder: CapsuleEncoder::new(),
        }
    }

    /// The D4 entry point: register `etype` on `stream` and append one event of
    /// it, at `expected_version`, optionally under a `dedupe` key.
    fn register_and_append(
        &mut self,
        stream: u64,
        etype: u32,
        expected_version: u64,
        dedupe: Option<Vec<u8>>,
        payload: &[u8],
    ) -> Outcome {
        // Idempotency first: a matching dedupe key returns the original commit.
        if let Some(k) = &dedupe
            && let Some(existing) = self.dedupe.get(k)
        {
            return Outcome::Idempotent(*existing);
        }
        // Expected-version check against the recovered head.
        let head = self.heads.get(&stream).copied().unwrap_or(0);
        if head != expected_version {
            return Outcome::Conflict {
                expected: expected_version,
                actual:   head,
            };
        }

        // Build the capsule: register the event type (if new), optional dedupe
        // control, then one event of it.
        let mut controls: Vec<ControlRecord> = Vec::new();
        if self.registry.state.event_type_name(etype).is_none() {
            controls.push(ControlRecord::EventTypeRegistered {
                event_type_id:          etype,
                codec_id:               1,
                current_schema_version: 1,
                schema_fingerprint:     [7; 32],
                name:                   format!("T{etype}"),
            });
        }
        if let Some(k) = &dedupe {
            controls.push(ControlRecord::DedupeKey {
                scope_kind: mess_log::v4::format::DEDUPE_SCOPE_STREAM,
                scope_id:   stream,
                key:        k.clone(),
            });
        }
        let sfs = [Subframe::plain(etype, 1, 1, payload)];
        let input = CapsuleInput {
            segment_epoch:        self.epoch,
            batch_id:             self.next_batch_id,
            first_global_pos:     self.next_gp,
            stream_id:            stream,
            category_id:          0,
            first_stream_version: expected_version,
            crypto_chain:         None,
            controls:             &controls,
            subframes:            &sfs,
        };
        let bytes = self.encoder.encode(&input).unwrap().to_vec();
        let commit = Commit {
            batch_id:         self.next_batch_id,
            first_global_pos: self.next_gp,
        };
        // Append + fdatasync (durable).
        let file = self.fs.open(self.path, OpenOpts::create_rw()).unwrap();
        let mut off = self.write_off;
        let mut buf = &bytes[..];
        while !buf.is_empty() {
            let n = file.pwrite(off, buf).unwrap();
            off += n as u64;
            buf = &buf[n..];
        }
        file.fdatasync().unwrap();

        // Update in-memory state.
        for ctl in &controls {
            self.registry.apply(ctl).unwrap();
        }
        self.write_off += bytes.len() as u64;
        self.next_batch_id += 1;
        self.next_gp += 1;
        *self.heads.entry(stream).or_insert(0) = expected_version + 1;
        if let Some(k) = dedupe {
            self.dedupe.insert(k, commit);
        }
        Outcome::Committed(commit)
    }

    fn segment_len(&self) -> u64 { read_all(self.fs, self.path).len() as u64 }
}

fn read_all(fs: &impl Fs, path: &Path) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

// ---------------------------------------------------------------------------
// D4 (a): matching dedupe key -> original commit, appends nothing.
// ---------------------------------------------------------------------------

#[test]
fn d4a_retry_with_matching_dedupe_key_returns_original_and_appends_nothing() {
    let rt = SimRuntime::new(1);
    let fs = rt.fs();
    let path = Path::new("/d4a.seg");
    let epoch = 5;

    // First attempt: durable, but the ack is "lost" (we drop this engine).
    let commit0;
    let key = b"idem-key-A".to_vec();
    {
        let mut engine = Shim::create(&fs, path, epoch);
        let out =
            engine.register_and_append(9, 5, 0, Some(key.clone()), b"ev0");
        commit0 = match out {
            Outcome::Committed(c) => c,
            other => panic!("first attempt should commit, got {other:?}"),
        };
    }

    // Crash + restart: recover the durable-but-unacked capsule.
    let mut engine2 = Shim::recover(&fs, path, epoch);
    let len_before = engine2.segment_len();
    assert_eq!(engine2.next_batch_id, 1, "recovered the durable capsule");
    assert!(engine2.dedupe.contains_key(&key), "dedupe key recovered");

    // Retry the SAME register+append with the matching dedupe key.
    let out = engine2.register_and_append(9, 5, 0, Some(key.clone()), b"ev0");
    assert_eq!(
        out,
        Outcome::Idempotent(commit0),
        "retry must return the ORIGINAL commit"
    );
    assert_eq!(
        engine2.segment_len(),
        len_before,
        "retry must append NOTHING (segment length unchanged)"
    );
    assert_eq!(engine2.next_batch_id, 1, "no new capsule");
}

// ---------------------------------------------------------------------------
// D4 (b): no dedupe key -> expected-version conflict, appends nothing.
// ---------------------------------------------------------------------------

#[test]
fn d4b_retry_without_dedupe_key_conflicts_and_appends_nothing() {
    let rt = SimRuntime::new(2);
    let fs = rt.fs();
    let path = Path::new("/d4b.seg");
    let epoch = 5;

    // First attempt: durable, ack lost. No dedupe key this time.
    {
        let mut engine = Shim::create(&fs, path, epoch);
        let out = engine.register_and_append(9, 5, 0, None, b"ev0");
        assert!(matches!(out, Outcome::Committed(_)));
    }

    // Crash + restart: the stream head is now version 1.
    let mut engine2 = Shim::recover(&fs, path, epoch);
    let len_before = engine2.segment_len();
    assert_eq!(engine2.heads.get(&9).copied(), Some(1), "head recovered to 1");

    // Retry the SAME append expecting version 0 (what the client believed).
    let out = engine2.register_and_append(9, 5, 0, None, b"ev0");
    assert_eq!(
        out,
        Outcome::Conflict { expected: 0, actual: 1 },
        "retry without a dedupe key must be an expected-version conflict"
    );
    assert_eq!(
        engine2.segment_len(),
        len_before,
        "conflict must append NOTHING"
    );
    assert_eq!(engine2.next_batch_id, 1, "no new capsule");
}

// ---------------------------------------------------------------------------
// Guard: the registry seam does NOT weaken AlreadyRegistered (review S4/V6).
// Two capsules registering the same event type -> the second is rejected on
// recovery (the scan stops), never silently accepted.
// ---------------------------------------------------------------------------

#[test]
fn registry_view_does_not_weaken_already_registered() {
    let rt = SimRuntime::new(3);
    let fs = rt.fs();
    let path = Path::new("/dup.seg");
    CapsuleWriter::create(&fs, path, SegmentParamsV4::new(1, 0, 5, 0))
        .unwrap()
        .close()
        .unwrap();
    let mut img = read_all(&fs, path);
    let mut enc = CapsuleEncoder::new();
    for batch_id in 0..2u64 {
        let controls = [ControlRecord::EventTypeRegistered {
            event_type_id:          5,
            codec_id:               1,
            current_schema_version: 1,
            schema_fingerprint:     [0; 32],
            name:                   "T5".to_string(),
        }];
        let input = CapsuleInput {
            segment_epoch: 5,
            batch_id,
            first_global_pos: 0,
            stream_id: 0,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &controls,
            subframes: &[],
        };
        img.extend_from_slice(enc.encode(&input).unwrap());
    }
    let (rec, _v) = scan_v4_image(&img, RegistryStateView::default());
    assert_eq!(
        rec.accepted.len(),
        1,
        "the second duplicate registration must stop the scan \
         (AlreadyRegistered)"
    );
}
