//! Randomized differential test: the microblock candidates must agree with
//! the REAL `ActiveIndex` (F0) on every read, for the same append
//! interleavings — stream head, version resolve (all three search modes),
//! paged range reads (both descent modes), full stream reads, global seeks,
//! and watermark clamping (including the defensive skip of a batch handed
//! above its group watermark).

#![cfg(not(loom))]

use active_microblocks::micro::MicroIndex;
use active_microblocks::workload::Rng;
use mess_index::{ActiveIndex, BatchEntry, EventPtr};

const SEGMENT: u64 = 1;

struct World {
    f0:       ActiveIndex,
    f1:       MicroIndex, // stride 1: exact global-seek equivalence
    f8:       MicroIndex, // stride 8: sparse-seek containment property
    rng:      Rng,
    versions: Vec<u64>,
    global:   u64,
    offset:   u64,
}

impl World {
    fn new(seed: u64, n_streams: usize) -> Self {
        World {
            f0:       ActiveIndex::new(),
            f1:       MicroIndex::new(SEGMENT, 1),
            f8:       MicroIndex::new(SEGMENT, 8),
            rng:      Rng::new(seed),
            versions: vec![0u64; n_streams],
            global:   0,
            offset:   0,
        }
    }

    fn make_batch(&mut self) -> BatchEntry {
        let sid = self.rng.below(self.versions.len() as u64);
        let frames = 1 + self.rng.below(5) as u32;
        // Occasionally jump the offset by >4 GiB (forces a delta escape) or
        // stamp a foreign segment id (forces a segment escape).
        if self.rng.below(37) == 0 {
            self.offset += 5 << 30;
        }
        let segment = if self.rng.below(29) == 0 { 7 } else { SEGMENT };
        let b = BatchEntry {
            stream_id:            sid,
            first_stream_version: self.versions[sid as usize],
            frame_count:          frames,
            first_global_pos:     self.global,
            ptr:                  EventPtr { segment_id: segment, offset: self.offset },
        };
        self.versions[sid as usize] += u64::from(frames);
        self.global += u64::from(frames);
        self.offset += 64 + u64::from(frames) * 120;
        b
    }

    fn apply_group(&mut self) {
        let n = 1 + self.rng.below(8) as usize;
        let group: Vec<BatchEntry> = (0..n).map(|_| self.make_batch()).collect();
        let full_wm = group.last().unwrap().end_pos();
        // The defensive watermark skip is release-only behavior on both
        // sides (debug builds assert against a batch above the watermark —
        // the incumbent's documented contract), so exercise it only there.
        if cfg!(not(debug_assertions)) && n >= 2 && self.rng.below(10) == 0 {
            // Exercise the defensive watermark skip identically on both
            // sides: hand the whole group with a watermark that only covers
            // a prefix (the suffix is skipped), then commit the suffix.
            let cut = 1 + self.rng.below(n as u64 - 1) as usize;
            let short_wm = group[cut - 1].end_pos();
            self.f0.apply_committed(short_wm, &group);
            self.f1.apply_committed(short_wm, &group);
            self.f8.apply_committed(short_wm, &group);
            self.f0.apply_committed(full_wm, &group[cut..]);
            self.f1.apply_committed(full_wm, &group[cut..]);
            self.f8.apply_committed(full_wm, &group[cut..]);
        } else {
            self.f0.apply_committed(full_wm, &group);
            self.f1.apply_committed(full_wm, &group);
            self.f8.apply_committed(full_wm, &group);
        }
    }

    fn check(&mut self) {
        assert_eq!(self.f0.applied_end(), self.f1.applied_end());
        assert_eq!(self.f0.applied_end(), self.f8.applied_end());
        let n = self.versions.len() as u64;
        for _ in 0..4 {
            // Include unknown streams (beyond the id space).
            let sid = self.rng.below(n + 2);
            let head = self.f0.stream_head(sid);
            assert_eq!(head, self.f1.stream_head(sid), "head sid={sid}");
            assert_eq!(head, self.f8.stream_head(sid), "head sid={sid}");

            let hi = head.map_or(3, |h| h + 3);
            let v = self.rng.below(hi.max(1));
            let want = self.f0.resolve(sid, v);
            assert_eq!(want, self.f1.resolve_f1(sid, v), "f1 sid={sid} v={v}");
            assert_eq!(want, self.f1.resolve_f2(sid, v), "f2 sid={sid} v={v}");
            assert_eq!(want, self.f1.resolve_f3(sid, v), "f3 sid={sid} v={v}");

            let from = self.rng.below(hi.max(1));
            let max = [0usize, 1, 3, 17, 1000][self.rng.below(5) as usize];
            let want = self.f0.stream_entries_from(sid, from, max);
            assert_eq!(
                want,
                self.f1.stream_entries_from::<false>(sid, from, max),
                "entries_from sid={sid} from={from} max={max}"
            );
            assert_eq!(
                want,
                self.f1.stream_entries_from::<true>(sid, from, max),
                "entries_from(skip) sid={sid} from={from} max={max}"
            );
            assert_eq!(self.f0.stream_entries(sid), self.f1.stream_entries(sid));
        }

        // Global seeks (dense log: every pos below applied_end resolves).
        let w = self.f0.applied_end();
        if w > 0 {
            let g = self.f0.global_committed();
            for _ in 0..4 {
                let pos = self.rng.below(w);
                let want = self
                    .f0
                    .global_range(pos, 1)
                    .first()
                    .map(|e| (e.first_global_pos, e.ptr.offset));
                // Stride 1: exactly the containing batch.
                assert_eq!(want, self.f1.global_seek(pos), "seek pos={pos}");
                // Stride 8: a checkpoint at or below the containing batch,
                // matching a real batch, within 8 batches of the target.
                let (cpos, coff) =
                    self.f8.global_seek(pos).expect("dense log: seek must land");
                assert!(cpos <= pos);
                let ci = g
                    .iter()
                    .position(|e| e.first_global_pos == cpos && e.ptr.offset == coff)
                    .expect("checkpoint must be a real batch");
                let ti = g
                    .iter()
                    .position(|e| e.first_global_pos <= pos && e.end_pos() > pos)
                    .expect("containing batch");
                assert!(ci <= ti && ti - ci < 8, "sparse seek within stride");
            }
        }
    }
}

fn run(seed: u64) {
    let mut w = World::new(seed, 48);
    for step in 0..2500 {
        w.apply_group();
        if step % 3 == 0 {
            w.check();
        }
    }
    w.check();
    let stats = w.f1.stats();
    assert!(stats.escapes > 0, "workload must exercise escape entries");
    assert!(stats.blocks > 0);
}

#[test]
fn differential_seed_1() {
    run(0xA11CE_1);
}

#[test]
fn differential_seed_2() {
    run(0xB0B_2);
}

#[test]
fn differential_seed_3() {
    run(0xC0DE_3);
}

#[test]
fn differential_seed_4() {
    run(0xD00D_4);
}

/// Deep chains on one hot stream: hundreds of blocks, resolves at every
/// depth, all three search modes (skip chain correctness at depth).
#[test]
fn differential_hot_deep() {
    let f0 = ActiveIndex::new();
    let f1 = MicroIndex::new(SEGMENT, 8);
    let mut global = 0u64;
    let mut offset = 0u64;
    let n = 20_000u64; // 1-event batches -> 625 blocks deep
    for v in 0..n {
        let b = BatchEntry {
            stream_id:            0,
            first_stream_version: v,
            frame_count:          1,
            first_global_pos:     global,
            ptr:                  EventPtr { segment_id: SEGMENT, offset },
        };
        global += 1;
        offset += 184;
        f0.apply_committed(global, &[b]);
        f1.apply_committed(global, &[b]);
    }
    assert_eq!(f0.stream_head(0), f1.stream_head(0));
    let mut rng = Rng::new(9);
    for _ in 0..4000 {
        let v = rng.below(n + 2);
        let want = f0.resolve(0, v);
        assert_eq!(want, f1.resolve_f1(0, v), "v={v}");
        assert_eq!(want, f1.resolve_f2(0, v), "v={v}");
        assert_eq!(want, f1.resolve_f3(0, v), "v={v}");
        let from = rng.below(n);
        let max = [1usize, 5, 64, 4096][rng.below(4) as usize];
        assert_eq!(
            f0.stream_entries_from(0, from, max),
            f1.stream_entries_from::<true>(0, from, max),
            "from={from} max={max}"
        );
    }
}
