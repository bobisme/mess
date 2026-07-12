//! bn-9mw Spike E: v4 recovery — writer round-trips, prelude-first registry
//! resolution, control-only cursor semantics, v3/v4 version dispatch, and the
//! full negative-decode matrix (every physical + protocol rejection path).

use std::collections::HashSet;
use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::runtime::{FileHandle, Fs, OpenOpts, Runtime, SimRuntime};
use mess_log::v4::capsule::{
    CapsuleDecodeError, CapsuleEncoder, CapsuleInput, decode_capsule,
};
use mess_log::v4::control::ControlRecord;
use mess_log::v4::format::*;
use mess_log::v4::recover::{
    NullRegistryView, RegistryView, ScanStopV4, SegmentFormat,
    directory_contains_v4_segment, peek_segment_format, recover_v4_segment,
    recover_v4_segment_physical, scan_v4_image, scan_v4_image_physical,
};
use mess_log::v4::writer::{CapsuleSpec, CapsuleWriter, SegmentParamsV4};

// ---------------------------------------------------------------------------
// A test RegistryView double (NOT a RegistryState reimplementation — the real
// semantics live in mess-store). It tracks only which ids have been introduced,
// enough to exercise the *physical* prelude-first ordering: a control
// introduces an id, then an event in the same capsule resolves against it.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct SetRegistryView {
    event_types: HashSet<u32>,
    streams:     HashSet<u64>,
    categories:  HashSet<u64>,
}

// The payloads are surfaced only through the `Debug` string the scanner
// embeds in `ScanStopV4::RegistryRejected`; the fields are "read" there.
#[derive(Debug)]
#[allow(dead_code)]
enum SetReject {
    DuplicateEventType(u32),
    UnknownCategory(u64),
}

impl RegistryView for SetRegistryView {
    type Reject = SetReject;

    fn apply(&mut self, control: &ControlRecord) -> Result<(), SetReject> {
        match control {
            ControlRecord::CategoryRegistered { category_id, .. } => {
                self.categories.insert(*category_id);
            }
            ControlRecord::StreamRegistered {
                stream_id, category_id, ..
            } => {
                if *category_id != 0 && !self.categories.contains(category_id) {
                    return Err(SetReject::UnknownCategory(*category_id));
                }
                self.streams.insert(*stream_id);
            }
            ControlRecord::EventTypeRegistered { event_type_id, .. } => {
                // Mirror RegistryState's non-weakened AlreadyRegistered: a
                // second registration of the same id rejects.
                if !self.event_types.insert(*event_type_id) {
                    return Err(SetReject::DuplicateEventType(*event_type_id));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn event_type_resolves(&self, event_type_id: u32) -> bool {
        // id 0 is the reserved bootstrap type (always resolves).
        event_type_id == 0 || self.event_types.contains(&event_type_id)
    }
}

fn enc(input: &CapsuleInput) -> Vec<u8> {
    CapsuleEncoder::new().encode(input).expect("encode").to_vec()
}

// ---------------------------------------------------------------------------
// Writer -> recovery round-trips
// ---------------------------------------------------------------------------

fn seg_path() -> &'static Path { Path::new("/v4.seg") }

#[test]
fn writer_roundtrip_control_only_events_only_mixed() {
    let rt = SimRuntime::new(1);
    let fs = rt.fs();
    let path = seg_path();
    let mut w =
        CapsuleWriter::create(&fs, path, SegmentParamsV4::new(1, 0, 5, 0))
            .unwrap();

    // capsule 0: control-only (register category+stream+event type).
    let controls0 = vec![
        ControlRecord::CategoryRegistered {
            category_id: 2,
            name:        "orders".to_string(),
        },
        ControlRecord::StreamRegistered {
            stream_id:   9,
            category_id: 2,
            name:        "s9".to_string(),
        },
        ControlRecord::EventTypeRegistered {
            event_type_id:          3,
            codec_id:               1,
            current_schema_version: 1,
            schema_fingerprint:     [1; 32],
            name:                   "T".to_string(),
        },
    ];
    let r0 = w
        .append(&CapsuleSpec {
            stream_id:            0,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &controls0,
            subframes:            &[],
        })
        .unwrap();
    w.sync().unwrap();
    assert_eq!(r0.batch_id, 0);
    assert_eq!(r0.first_global_pos, 0);
    assert_eq!(r0.event_count, 0);

    // capsule 1: events-only (2 events on stream 9).
    let sfs1 =
        [Subframe::plain(3, 1, 1, b"a"), Subframe::plain(3, 1, 1, b"bb")];
    let r1 = w
        .append(&CapsuleSpec {
            stream_id:            9,
            category_id:          2,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &sfs1,
        })
        .unwrap();
    w.sync().unwrap();
    assert_eq!(r1.batch_id, 1);
    assert_eq!(r1.first_global_pos, 0); // control-only capsule didn't advance gp

    // capsule 2: mixed (dedupe control + 1 event).
    let controls2 = vec![ControlRecord::DedupeKey {
        scope_kind: DEDUPE_SCOPE_STREAM,
        scope_id:   9,
        key:        b"k".to_vec(),
    }];
    let sfs2 = [Subframe::plain(3, 1, 1, b"c")];
    let r2 = w
        .append(&CapsuleSpec {
            stream_id:            9,
            category_id:          2,
            first_stream_version: 2,
            crypto_chain:         None,
            controls:             &controls2,
            subframes:            &sfs2,
        })
        .unwrap();
    w.close().unwrap();
    assert_eq!(r2.batch_id, 2);
    assert_eq!(r2.first_global_pos, 2);

    // Recover with the semantic seam.
    let (rec, view) =
        recover_v4_segment(&fs, path, SetRegistryView::default()).unwrap();
    assert_eq!(rec.stop, ScanStopV4::EndOfSegment);
    assert_eq!(rec.accepted.len(), 3);
    assert_eq!(rec.next_batch_id, 3);
    assert_eq!(rec.next_global_pos, 3); // 0 + 0 + 2 + 1
    assert!(rec.accepted[0].control_only);
    assert!(!rec.accepted[1].control_only);
    // CommitCursor advanced past all 3 capsules; global position is 3.
    assert_eq!(rec.commit_cursor.batch_id, 3);
    assert_eq!(rec.commit_cursor.global_position, 3);
    // The seam saw the registrations.
    assert!(view.event_types.contains(&3));

    // Idempotence.
    let rec2 = recover_v4_segment_physical(&fs, path).unwrap();
    let rec1phys = scan_v4_image_physical(&read_all(&fs, path));
    assert_eq!(rec2, rec1phys);
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
// Prelude-first: register-and-use in the same capsule
// ---------------------------------------------------------------------------

#[test]
fn register_and_use_event_type_in_same_capsule_accepted() {
    // One capsule: register event type 42, then an event of type 42.
    let controls = vec![ControlRecord::EventTypeRegistered {
        event_type_id:          42,
        codec_id:               1,
        current_schema_version: 1,
        schema_fingerprint:     [0; 32],
        name:                   "T42".to_string(),
    }];
    let sfs = [Subframe::plain(42, 1, 1, b"x")];
    let bytes = capsule_segment(&[CapsuleInput {
        segment_epoch:        5,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &sfs,
    }]);
    let (rec, _v) = scan_v4_image(&bytes, SetRegistryView::default());
    assert_eq!(rec.stop, ScanStopV4::EndOfSegment);
    assert_eq!(rec.accepted.len(), 1);
}

#[test]
fn use_unregistered_event_type_rejected() {
    // An event of type 99 with no control registering it → EventTypeUnresolved.
    let sfs = [Subframe::plain(99, 1, 1, b"x")];
    let bytes = capsule_segment(&[CapsuleInput {
        segment_epoch:        5,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &[],
        subframes:            &sfs,
    }]);
    let (rec, _v) = scan_v4_image(&bytes, SetRegistryView::default());
    assert_eq!(rec.accepted.len(), 0);
    assert_eq!(rec.stop, ScanStopV4::EventTypeUnresolved { event_type_id: 99 });
}

#[test]
fn duplicate_registration_rejected_not_weakened() {
    // Two capsules each register event type 7 → the second is AlreadyRegistered
    // in the committed view. (The seam must NOT weaken this — review S4/V6.)
    let ctl = |id| {
        vec![ControlRecord::EventTypeRegistered {
            event_type_id:          id,
            codec_id:               1,
            current_schema_version: 1,
            schema_fingerprint:     [0; 32],
            name:                   format!("T{id}"),
        }]
    };
    let c0 = ctl(7);
    let c1 = ctl(7);
    let bytes = capsule_segment(&[
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            0,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &c0,
            subframes:            &[],
        },
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             1,
            first_global_pos:     0,
            stream_id:            0,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &c1,
            subframes:            &[],
        },
    ]);
    let (rec, _v) = scan_v4_image(&bytes, SetRegistryView::default());
    assert_eq!(rec.accepted.len(), 1, "second dup registration must stop scan");
    assert!(matches!(rec.stop, ScanStopV4::RegistryRejected { .. }));
}

// ---------------------------------------------------------------------------
// Build a v4 segment image (header + capsules) in memory for scan tests.
// ---------------------------------------------------------------------------

fn capsule_segment(inputs: &[CapsuleInput]) -> Vec<u8> {
    // Lay down a real v4 header (epoch/base from the first input), then append
    // raw encoded capsules so tests control batch_id/position/epoch exactly.
    let epoch = inputs.first().map(|i| i.segment_epoch).unwrap_or(1);
    let base = inputs.first().map(|i| i.first_global_pos).unwrap_or(0);
    segment_with_header(1, base, epoch, inputs)
}

/// A v4 segment image with the given header params and raw appended capsules.
fn segment_with_header(
    segment_id: u64,
    base: u64,
    epoch: u64,
    inputs: &[CapsuleInput],
) -> Vec<u8> {
    let rt = SimRuntime::new(99);
    let fs = rt.fs();
    let path = Path::new("/mk.seg");
    CapsuleWriter::create(
        &fs,
        path,
        SegmentParamsV4::new(segment_id, base, epoch, 0),
    )
    .unwrap()
    .close()
    .unwrap();
    let mut img = read_all(&fs, path);
    for input in inputs {
        img.extend_from_slice(&enc(input));
    }
    img
}

// ---------------------------------------------------------------------------
// v3 / v4 segment version dispatch (§22)
// ---------------------------------------------------------------------------

#[test]
fn version_detection_and_write_mode_refusal() {
    let rt = SimRuntime::new(2);
    let fs = rt.fs();

    // A v4 segment.
    let v4p = Path::new("/seg-v4");
    CapsuleWriter::create(&fs, v4p, SegmentParamsV4::new(1, 0, 1, 0))
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(peek_segment_format(&fs, v4p).unwrap(), SegmentFormat::V4);

    // A v3 segment via the production writer.
    use mess_log::writer::{SegmentParams, SegmentWriter};
    let v3p = Path::new("/seg-v3");
    SegmentWriter::create(&fs, v3p, SegmentParams::new(2, 0, 1, 0))
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(peek_segment_format(&fs, v3p).unwrap(), SegmentFormat::V3);

    // A v3-only writer MUST refuse write mode on a dir containing a v4 segment.
    assert!(directory_contains_v4_segment(&fs, &[v3p, v4p]).unwrap());
    assert!(!directory_contains_v4_segment(&fs, &[v3p]).unwrap());

    // A torn/absent header names no version.
    let missing = Path::new("/nope");
    // (an empty seeded file)
    fs.open(missing, OpenOpts::create_rw()).unwrap();
    assert_eq!(
        peek_segment_format(&fs, missing).unwrap(),
        SegmentFormat::NoValidHeader
    );
}

// ---------------------------------------------------------------------------
// Negative decode matrix — every physical + protocol rejection path
// ---------------------------------------------------------------------------

/// A valid single-event capsule to corrupt.
fn base_capsule() -> Vec<u8> {
    let sfs = [Subframe::plain(0, 0, 0, b"payload")];
    enc(&CapsuleInput {
        segment_epoch:        5,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &[],
        subframes:            &sfs,
    })
}

fn put_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
fn put_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}

#[test]
fn decode_rejects_bad_magic() {
    let mut b = base_capsule();
    put_u32(&mut b, CH_MAGIC_OFF_TEST, 0xDEAD_BEEF);
    assert_eq!(
        decode_capsule(&b, 0).unwrap_err(),
        CapsuleDecodeError::BadMagic
    );
}

#[test]
fn decode_rejects_bad_version() {
    let mut b = base_capsule();
    put_u16(&mut b, 4, 3); // v3 in a v4 slot
    assert!(matches!(
        decode_capsule(&b, 0),
        Err(CapsuleDecodeError::BadVersion(3))
    ));
}

#[test]
fn decode_rejects_unknown_flags() {
    let mut b = base_capsule();
    put_u16(&mut b, 6, 0x8000); // undefined physical flag
    fix_crc(&mut b);
    assert!(matches!(
        decode_capsule(&b, 0),
        Err(CapsuleDecodeError::UnknownFlags(_))
    ));
}

#[test]
fn decode_rejects_unknown_logical_flags() {
    let mut b = base_capsule();
    put_u32(&mut b, 88, 0x8000_0000); // undefined logical flag
    fix_crc(&mut b);
    assert!(matches!(
        decode_capsule(&b, 0),
        Err(CapsuleDecodeError::UnknownLogicalFlags(_))
    ));
}

#[test]
fn decode_rejects_reserved_nonzero() {
    let mut b = base_capsule();
    put_u32(&mut b, 84, 1); // reserved_hdr (dropped header_crc slot)
    fix_crc(&mut b);
    assert_eq!(
        decode_capsule(&b, 0).unwrap_err(),
        CapsuleDecodeError::ReservedNonzero
    );
}

#[test]
fn decode_rejects_bad_crc() {
    let mut b = base_capsule();
    // Flip a covered payload byte without fixing the CRC.
    let n = b.len();
    b[n - CAPSULE_MARKER_LEN - 1] ^= 0xFF;
    assert_eq!(decode_capsule(&b, 0).unwrap_err(), CapsuleDecodeError::BadCrc);
}

#[test]
fn decode_rejects_bad_marker_echo() {
    let mut b = base_capsule();
    let m = b.len() - CAPSULE_MARKER_LEN;
    // Corrupt the total_len_echo but keep the CRC consistent so ONLY the marker
    // check fires.
    put_u32(&mut b, m + 16, 0xFFFF_FFFF);
    fix_crc(&mut b);
    assert_eq!(
        decode_capsule(&b, 0).unwrap_err(),
        CapsuleDecodeError::BadMarker
    );
}

#[test]
fn decode_rejects_torn_header() {
    let b = base_capsule();
    let short = &b[..CAPSULE_HEADER_LEN - 1];
    assert!(matches!(
        decode_capsule(short, 0),
        Err(CapsuleDecodeError::TornHeader { .. })
    ));
}

#[test]
fn decode_rejects_control_only_flag_mismatch() {
    // event_count > 0 but CONTROL_ONLY set.
    let mut b = base_capsule();
    put_u32(&mut b, 88, LFLAG_CONTROL_ONLY);
    fix_crc(&mut b);
    assert_eq!(
        decode_capsule(&b, 0).unwrap_err(),
        CapsuleDecodeError::ControlOnlyFlagMismatch
    );
}

#[test]
fn decode_rejects_unknown_control_kind() {
    // Build a control-only capsule then rewrite the control kind to unknown.
    let controls = [ControlRecord::CategoryRegistered {
        category_id: 1,
        name:        "x".to_string(),
    }];
    let mut b = enc(&CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &[],
    });
    // Control region begins at CAPSULE_HEADER_LEN; kind is its first u16.
    put_u16(&mut b, CAPSULE_HEADER_LEN, 0x7777);
    fix_crc(&mut b);
    assert!(matches!(
        decode_capsule(&b, 0),
        Err(CapsuleDecodeError::Control(_))
    ));
}

#[test]
fn decode_rejects_unknown_control_version() {
    let controls = [ControlRecord::CategoryRegistered {
        category_id: 1,
        name:        "x".to_string(),
    }];
    let mut b = enc(&CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &[],
    });
    put_u16(&mut b, CAPSULE_HEADER_LEN + 2, 99); // version field
    fix_crc(&mut b);
    assert!(matches!(
        decode_capsule(&b, 0),
        Err(CapsuleDecodeError::Control(_))
    ));
}

// ---------------------------------------------------------------------------
// Protocol-level rejection (recovery scanner)
// ---------------------------------------------------------------------------

#[test]
fn recovery_stops_on_batch_id_gap() {
    // capsule 0 ok, capsule 1 has batch_id 5 (should be 1) → BatchIdGap, no
    // resync.
    let bytes = capsule_segment(&[
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            9,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &[Subframe::plain(0, 0, 0, b"a")],
        },
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             5, // GAP
            first_global_pos:     1,
            stream_id:            9,
            category_id:          0,
            first_stream_version: 1,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &[Subframe::plain(0, 0, 0, b"b")],
        },
    ]);
    let rec = scan_v4_image_physical(&bytes);
    assert_eq!(rec.accepted.len(), 1);
    assert_eq!(rec.stop, ScanStopV4::BatchIdGap { expected: 1, found: 5 });
}

#[test]
fn recovery_stops_on_position_gap() {
    let bytes = capsule_segment(&[
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            9,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &[Subframe::plain(0, 0, 0, b"a")],
        },
        CapsuleInput {
            segment_epoch:        5,
            batch_id:             1,
            first_global_pos:     99, // should be 1
            stream_id:            9,
            category_id:          0,
            first_stream_version: 1,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &[Subframe::plain(0, 0, 0, b"b")],
        },
    ]);
    let rec = scan_v4_image_physical(&bytes);
    assert_eq!(rec.accepted.len(), 1);
    assert_eq!(rec.stop, ScanStopV4::PositionGap { expected: 1, found: 99 });
}

#[test]
fn recovery_stops_on_epoch_mismatch() {
    // Header epoch 5, but the capsule stamps segment_epoch 999 (a stale prior
    // generation in recycled space) → A9 EpochMismatch, nothing accepted.
    let img = segment_with_header(
        1,
        0,
        5,
        &[CapsuleInput {
            segment_epoch:        999,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            9,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &[Subframe::plain(0, 0, 0, b"a")],
        }],
    );
    let rec = scan_v4_image_physical(&img);
    assert_eq!(rec.accepted.len(), 0);
    assert_eq!(rec.stop, ScanStopV4::EpochMismatch);
}

#[test]
fn recovery_null_view_accepts_everything_physical() {
    let bytes = capsule_segment(&[CapsuleInput {
        segment_epoch:        5,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &[],
        subframes:            &[Subframe::plain(12345, 0, 0, b"a")],
    }]);
    let (rec, _v) = scan_v4_image(&bytes, NullRegistryView);
    assert_eq!(rec.accepted.len(), 1, "null view resolves any event type");
    assert_eq!(rec.stop, ScanStopV4::EndOfSegment);
}

// helpers -------------------------------------------------------------------

const CH_MAGIC_OFF_TEST: usize = 0;

/// Recompute + rewrite both CRC fields after a mutation, so a test isolates the
/// specific non-CRC rejection it targets.
fn fix_crc(b: &mut [u8]) {
    let crc = mess_log::v4::capsule::capsule_crc(b);
    put_u32(b, CAPSULE_HEADER_CRC_OFF, crc);
    let m = b.len() - CAPSULE_MARKER_LEN;
    put_u32(b, m + 24, crc);
}
