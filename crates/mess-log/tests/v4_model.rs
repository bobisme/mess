//! bn-9mw Spike E: the **exhaustive small-state crash model** (research/05 §9).
//!
//! Enumerate every small v4 segment shape and every durable-sector subset (the
//! "crash before/after every write/barrier" + sector-reorder adversary) over
//! both a zero and a stale-prior-generation background, recover, and assert the
//! safety properties that make the format admissible:
//!
//! - **P1 no control/event split** — every accepted capsule is byte-identical
//!   to the exact planned capsule; a partially-persisted capsule (only some of
//!   its controls/events durable) is NEVER accepted.
//! - **P2 no accepted duplicate/stale batch_id** — accepted `batch_id`s are
//!   exactly `0,1,2,…` and each equals its planned id.
//! - **P3 no global-position gap** — accepted `first_global_pos` is contiguous,
//!   advancing by `event_count`.
//! - **P4 no advance from control-only** — `next_global_pos` equals the sum of
//!   accepted `event_count`s (a control-only capsule moves the commit cursor
//!   but not the global position).
//! - **P5 registry-before-use holds** — for register-then-use workloads, the
//!   semantic ([`SetRegistryView`]) recovery accepts exactly the same prefix as
//!   the physical recovery (no accepted event references an unregistered type,
//!   and a byte-valid contiguous capsule is never *additionally* rejected).
//! - **P6 accepted prefix stable across repeated recovery** (idempotence).
//! - **P7 acked-implies-recovered** — if every sector of capsules `0..=k` is
//!   durable, all of `0..=k` are recovered.
//!
//! The clean segment header is always durable (the writer syncs it at create),
//! matching production; only the capsule sectors are subject to the crash
//! adversary.

use std::collections::HashSet;
use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::runtime::{FileHandle, Fs, OpenOpts, Runtime, SimRuntime};
use mess_log::v4::capsule::{CapsuleEncoder, CapsuleInput};
use mess_log::v4::control::ControlRecord;
use mess_log::v4::recover::{
    NullRegistryView, RecoveryV4, RegistryView, ScanStopV4, scan_v4_image,
    scan_v4_image_physical,
};
use mess_log::v4::writer::{CapsuleWriter, SegmentParamsV4};

const SEGMENT_HEADER_LEN: usize = 52;
const HEADER_EPOCH: u64 = 100;

// ---------------------------------------------------------------------------
// A planned capsule shape: how many controls, how many events.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct Shape {
    controls: u8,
    events:   u8,
}

/// The 8 valid single-capsule shapes: controls ∈ {0,1,2}, events ∈ {0,1,2},
/// sum >= 1.
fn shapes() -> Vec<Shape> {
    let mut v = Vec::new();
    for controls in 0..=2u8 {
        for events in 0..=2u8 {
            if controls + events >= 1 {
                v.push(Shape { controls, events });
            }
        }
    }
    v
}

/// Build the controls + subframes for one capsule of `shape`, at capsule index
/// `i`. Register-then-use: an event uses event type `100 + i` which the
/// capsule's first control (when present) registers.
fn build_capsule_content(shape: Shape, i: usize) -> (Vec<ControlRecord>, u32) {
    // Honest shapes: exactly `shape.controls` controls, exactly `shape.events`
    // events. When there is at least one control, the FIRST control registers
    // event type `100+i` and the events use it (register-then-use). With zero
    // controls the events use the reserved type `0` (always resolves), so a
    // "0 controls, N events" shape is a genuine events-only capsule.
    let etype = if shape.controls >= 1 { 100 + i as u32 } else { 0 };
    let mut controls = Vec::new();
    for c in 0..shape.controls {
        if c == 0 && shape.events > 0 {
            controls.push(ControlRecord::EventTypeRegistered {
                event_type_id:          etype,
                codec_id:               1,
                current_schema_version: 1,
                schema_fingerprint:     [i as u8; 32],
                name:                   format!("T{i}"),
            });
        } else {
            controls.push(ControlRecord::CategoryRegistered {
                category_id: (i as u64) * 10 + c as u64 + 1,
                name:        format!("c{i}_{c}"),
            });
        }
    }
    (controls, etype)
}

/// One planned segment: the clean image, per-capsule byte ranges, and
/// per-capsule (batch_id, first_global_pos, event_count, control_only).
struct Plan {
    clean:        Vec<u8>,
    ranges:       Vec<(usize, usize)>, // (offset, len) per capsule
    events:       Vec<u32>,
    control_only: Vec<bool>,
}

fn plan_segment(shape_combo: &[Shape]) -> Plan {
    let rt = SimRuntime::new(1);
    let fs = rt.fs();
    let path = Path::new("/plan.seg");
    CapsuleWriter::create(
        &fs,
        path,
        SegmentParamsV4::new(1, 0, HEADER_EPOCH, 0),
    )
    .unwrap()
    .close()
    .unwrap();
    let mut clean = read_all(&fs, path);
    let mut ranges = Vec::new();
    let mut events = Vec::new();
    let mut control_only = Vec::new();
    let mut enc = CapsuleEncoder::new();
    let mut batch_id = 0u64;
    let mut gpos = 0u64;
    for (i, shape) in shape_combo.iter().enumerate() {
        let (controls, etype) = build_capsule_content(*shape, i);
        let payloads: Vec<Vec<u8>> =
            (0..shape.events).map(|e| vec![0xAB, e]).collect();
        let sfs: Vec<Subframe> =
            payloads.iter().map(|p| Subframe::plain(etype, 1, 1, p)).collect();
        let control_only_cap = shape.events == 0;
        let input = CapsuleInput {
            segment_epoch: HEADER_EPOCH,
            batch_id,
            first_global_pos: gpos,
            stream_id: if control_only_cap { 0 } else { 9 },
            category_id: if control_only_cap { 0 } else { 0 },
            first_stream_version: 0,
            crypto_chain: None,
            controls: &controls,
            subframes: &sfs,
        };
        let bytes = enc.encode(&input).unwrap().to_vec();
        let off = clean.len();
        ranges.push((off, bytes.len()));
        clean.extend_from_slice(&bytes);
        events.push(shape.events as u32);
        control_only.push(control_only_cap);
        batch_id += 1;
        gpos += shape.events as u64;
    }
    Plan { clean, ranges, events, control_only }
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
// Backgrounds
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Bg {
    Zeros,
    Stale,
}

/// A stale prior-generation background of `len` bytes: valid-looking capsules
/// from epoch `HEADER_EPOCH - 1` (A9 must reject them) at a disjoint position
/// range, padded with 0xEE. Mirrors torn_matrix's `stale_background`.
fn stale_background(len: usize) -> Vec<u8> {
    let mut img = Vec::with_capacity(len);
    let mut enc = CapsuleEncoder::new();
    let mut batch_id = 500u64;
    let mut gpos = 1_000_000u64;
    while img.len() < len {
        let controls = [ControlRecord::CategoryRegistered {
            category_id: batch_id,
            name:        "stale".to_string(),
        }];
        let sfs = [Subframe::plain(7, 0, 0, b"stale-ev")];
        let input = CapsuleInput {
            segment_epoch: HEADER_EPOCH - 1, // prior generation
            batch_id,
            first_global_pos: gpos,
            stream_id: 77,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &controls,
            subframes: &sfs,
        };
        let bytes = enc.encode(&input).unwrap();
        if img.len() + bytes.len() > len {
            break;
        }
        img.extend_from_slice(bytes);
        batch_id += 1;
        gpos += 1;
    }
    img.resize(len, 0xEE);
    img
}

fn background(bg: Bg, len: usize) -> Vec<u8> {
    match bg {
        Bg::Zeros => vec![0u8; len],
        Bg::Stale => stale_background(len),
    }
}

/// Produce a crashed image: sectors in `durable` (bit set) take clean bytes,
/// the rest take background bytes. The header sectors are always durable.
fn crash_image(
    clean: &[u8],
    bg: &[u8],
    sector: usize,
    durable_content_sectors: u64,
) -> Vec<u8> {
    let mut out = vec![0u8; clean.len()];
    let n = clean.len();
    let mut s = 0usize;
    let mut off = 0usize;
    while off < n {
        let end = (off + sector).min(n);
        // Header always durable; content sectors gated by the bitmask.
        let durable = if off < SEGMENT_HEADER_LEN {
            true
        } else {
            let content_sector = s; // 0-based over ALL sectors incl header
            durable_content_sectors & (1u64 << content_sector) != 0
        };
        if durable {
            out[off..end].copy_from_slice(&clean[off..end]);
        } else {
            out[off..end].copy_from_slice(&bg[off..end]);
        }
        off = end;
        s += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Safety assertions
// ---------------------------------------------------------------------------

fn assert_safety(plan: &Plan, crashed: &[u8], ctx: &str) -> RecoveryV4 {
    let rec = scan_v4_image_physical(crashed);

    // P2/P3: accepted ids/positions are exactly the planned contiguous prefix.
    let mut expected_gpos = 0u64;
    for (idx, acc) in rec.accepted.iter().enumerate() {
        assert!(idx < plan.ranges.len(), "{ctx}: accepted more than planned");
        assert_eq!(acc.batch_id, idx as u64, "{ctx}: P2 batch_id");
        assert_eq!(
            acc.first_global_pos, expected_gpos,
            "{ctx}: P3 position contiguity"
        );
        // P1: byte-identity — the accepted capsule equals the planned bytes.
        let (off, len) = plan.ranges[idx];
        assert_eq!(acc.offset as usize, off, "{ctx}: P1 offset");
        assert_eq!(acc.total_len as usize, len, "{ctx}: P1 len");
        assert_eq!(
            &crashed[off..off + len],
            &plan.clean[off..off + len],
            "{ctx}: P1 accepted capsule bytes differ from planned — a \
             split/partial/stale capsule was accepted"
        );
        assert_eq!(acc.event_count, plan.events[idx], "{ctx}: event_count");
        assert_eq!(
            acc.control_only, plan.control_only[idx],
            "{ctx}: control_only"
        );
        expected_gpos += u64::from(acc.event_count);
    }
    // P4: global position advanced only by events.
    assert_eq!(rec.next_global_pos, expected_gpos, "{ctx}: P4 gp advance");
    assert_eq!(
        rec.next_batch_id,
        rec.accepted.len() as u64,
        "{ctx}: next_batch_id"
    );

    // P6: idempotence.
    let rec2 = scan_v4_image_physical(crashed);
    assert_eq!(rec, rec2, "{ctx}: P6 recovery not idempotent");

    // P5: the semantic view accepts exactly the same prefix (register-then-use
    // is well-formed by construction; the registry never additionally rejects a
    // byte-valid contiguous capsule, and never accepts an unregistered use).
    let (rec_set, _v) = scan_v4_image(crashed, SetRegistryView::default());
    assert_eq!(
        rec_set.accepted.len(),
        rec.accepted.len(),
        "{ctx}: P5 semantic recovery diverged from physical"
    );
    for (a, b) in rec_set.accepted.iter().zip(&rec.accepted) {
        assert_eq!(a.offset, b.offset, "{ctx}: P5 offset diverged");
    }

    rec
}

// A test RegistryView double (see v4_recovery.rs — not a RegistryState reimpl).
#[derive(Debug, Clone, Default)]
struct SetRegistryView {
    event_types: HashSet<u32>,
}

impl RegistryView for SetRegistryView {
    type Reject = std::convert::Infallible;

    fn apply(
        &mut self,
        control: &ControlRecord,
    ) -> Result<(), std::convert::Infallible> {
        if let ControlRecord::EventTypeRegistered { event_type_id, .. } =
            control
        {
            self.event_types.insert(*event_type_id);
        }
        Ok(())
    }

    fn event_type_resolves(&self, event_type_id: u32) -> bool {
        event_type_id == 0 || self.event_types.contains(&event_type_id)
    }
}

// ---------------------------------------------------------------------------
// Model A: 1..=2 capsules, EVERY durable-sector subset, both backgrounds.
// ---------------------------------------------------------------------------

fn content_sector_count(clean_len: usize, sector: usize) -> u32 {
    (clean_len.div_ceil(sector)) as u32
}

#[test]
fn model_a_full_subset_1_and_2_capsules() {
    let sector = 64usize;
    let shapes = shapes();
    let mut states = 0u64;

    // 1 capsule.
    for s0 in &shapes {
        run_full_subset(&[*s0], sector, &mut states);
    }
    // 2 capsules.
    for s0 in &shapes {
        for s1 in &shapes {
            run_full_subset(&[*s0, *s1], sector, &mut states);
        }
    }
    println!("model_a: {states} crash states enumerated (all safe)");
    assert!(states > 10_000, "model A should enumerate many states");
}

fn run_full_subset(combo: &[Shape], sector: usize, states: &mut u64) {
    let plan = plan_segment(combo);
    let total_sectors = content_sector_count(plan.clean.len(), sector);
    // Header sectors are forced-durable inside crash_image; enumerate the full
    // bitmask over ALL sectors (header bits are ignored there).
    assert!(total_sectors <= 20, "keep the subset space bounded");
    let bgs = [Bg::Zeros, Bg::Stale];
    for bg in bgs {
        let bgimg = background(bg, plan.clean.len());
        for mask in 0u64..(1u64 << total_sectors) {
            let crashed = crash_image(&plan.clean, &bgimg, sector, mask);
            let ctx = format!("A combo={combo:?} bg={bg:?} mask={mask:#x}");
            let rec = assert_safety(&plan, &crashed, &ctx);

            // P7: if all sectors of a capsule prefix are durable, they are all
            // recovered. Determine the durable byte-prefix from the mask.
            assert_p7(&plan, sector, mask, &rec, &ctx);
            *states += 1;
        }
    }
}

/// P7 acked-implies-recovered: for the maximal `k` such that every sector
/// overlapping capsules `0..=k` is durable AND all capsule bytes are clean,
/// those capsules must all appear in the accepted prefix.
fn assert_p7(
    plan: &Plan,
    sector: usize,
    mask: u64,
    rec: &RecoveryV4,
    ctx: &str,
) {
    let sector_durable = |s: usize| -> bool {
        if s * sector < SEGMENT_HEADER_LEN {
            return true;
        }
        mask & (1u64 << s) != 0
    };
    let mut fully_durable = 0usize;
    for (idx, (off, len)) in plan.ranges.iter().enumerate() {
        let first_s = off / sector;
        let last_s = (off + len - 1) / sector;
        let all = (first_s..=last_s).all(sector_durable);
        if all {
            fully_durable = idx + 1;
        } else {
            break;
        }
    }
    assert!(
        rec.accepted.len() >= fully_durable,
        "{ctx}: P7 — {} fully-durable capsules but only {} recovered",
        fully_durable,
        rec.accepted.len()
    );
}

// ---------------------------------------------------------------------------
// Model B: 3 capsules, durable PREFIX at each sector boundary + frontier tear,
// both backgrounds. Covers deeper chains ("crash after every barrier") plus
// intra-capsule tearing of the frontier capsule.
// ---------------------------------------------------------------------------

#[test]
fn model_b_three_capsule_prefix_and_frontier_tear() {
    let sector = 64usize;
    let shapes = shapes();
    let mut states = 0u64;
    // Sample a representative but broad set of 3-capsule combos: every shape in
    // each position paired with a rotating partner keeps it exhaustive over
    // single-position variation while bounding the cube. We still cover all 8^3
    // by nesting — 512 combos, each with O(sectors) prefixes × 2 bg.
    for s0 in &shapes {
        for s1 in &shapes {
            for s2 in &shapes {
                run_prefix_and_tear(&[*s0, *s1, *s2], sector, &mut states);
            }
        }
    }
    println!("model_b: {states} crash states enumerated (all safe)");
    assert!(states > 10_000, "model B should enumerate many states");
}

fn run_prefix_and_tear(combo: &[Shape], sector: usize, states: &mut u64) {
    let plan = plan_segment(combo);
    let total_sectors = content_sector_count(plan.clean.len(), sector);
    let bgs = [Bg::Zeros, Bg::Stale];
    for bg in bgs {
        let bgimg = background(bg, plan.clean.len());
        // Durable prefixes: first j sectors durable (j = 0..=total).
        for j in 0..=total_sectors {
            let prefix_mask = if j == 0 {
                0u64
            } else if j >= 64 {
                u64::MAX
            } else {
                (1u64 << j) - 1
            };
            let crashed = crash_image(&plan.clean, &bgimg, sector, prefix_mask);
            let ctx = format!("B combo={combo:?} bg={bg:?} prefix_j={j}");
            let rec = assert_safety(&plan, &crashed, &ctx);
            assert_p7(&plan, sector, prefix_mask, &rec, &ctx);
            *states += 1;

            // Frontier tear: additionally drop ONE interior sector within the
            // durable prefix (models a reordered/torn sector under the marker).
            if j >= 2 {
                for drop_s in (SEGMENT_HEADER_LEN / sector + 1)..(j as usize) {
                    let tear_mask = prefix_mask & !(1u64 << drop_s);
                    let crashed2 =
                        crash_image(&plan.clean, &bgimg, sector, tear_mask);
                    let ctx2 = format!(
                        "B-tear combo={combo:?} bg={bg:?} j={j} drop={drop_s}"
                    );
                    let rec2 = assert_safety(&plan, &crashed2, &ctx2);
                    assert_p7(&plan, sector, tear_mask, &rec2, &ctx2);
                    *states += 1;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// A clean-image sanity check: the full clean segment recovers ALL capsules.
// ---------------------------------------------------------------------------

#[test]
fn clean_image_recovers_all_capsules_every_shape() {
    for s0 in shapes() {
        for s1 in shapes() {
            let plan = plan_segment(&[s0, s1]);
            let rec = scan_v4_image_physical(&plan.clean);
            assert_eq!(rec.stop, ScanStopV4::EndOfSegment);
            assert_eq!(rec.accepted.len(), 2, "clean image recovers all");
            let (rec_set, _v) =
                scan_v4_image(&plan.clean, SetRegistryView::default());
            assert_eq!(rec_set.accepted.len(), 2);
        }
    }
    let _ = NullRegistryView;
}
