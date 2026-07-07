//! Correctness gates, run after every optimization (methodology requirement):
//!
//! 1. `verify_log`: full global scan of every segment with the UNCHANGED
//!    vertical_slice recovery scanner — every batch must validate (CRC,
//!    marker, A1 contiguity), event count and a commutative global checksum
//!    must match the workload, and per-stream payloads must arrive in exact
//!    version order with FNV checksums equal to the workload's.
//! 2. `verify_stream_reads`: the engine's own index read path must return the
//!    same per-stream (count, FNV) for a stream sample.
//! 3. Recovery gate (in main): reopen the directory with the baseline
//!    recovery path (scan last segment, rebuild/repair index) and re-run 1+2.

use std::collections::HashMap;
use std::fs::{self, File};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::seglog::{list_segments, scan, EventPtr, StopReason};
use crate::workload::{BatchSpec, STREAMS};

pub const FNV_SEED: u64 = 0xcbf2_9ce4_8422_2325;

pub fn fnv1a(mut h: u64, data: &[u8]) -> u64 {
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Ground truth derived from the workload itself (deterministic, seeded).
pub struct Expected {
    pub n_events: u64,
    pub global_ck: u64, // commutative: sum(first_byte + len) per event
    pub stream_fnv: Vec<u64>,
    pub counts: Vec<u64>,
}

pub fn expected_from(master: &[BatchSpec]) -> Expected {
    let mut global_ck = 0u64;
    let mut stream_fnv = vec![FNV_SEED; STREAMS as usize];
    let mut counts = vec![0u64; STREAMS as usize];
    let mut n = 0u64;
    for b in master {
        for p in &b.payloads {
            global_ck = global_ck.wrapping_add(p[0] as u64 + p.len() as u64);
            stream_fnv[b.stream as usize] = fnv1a(stream_fnv[b.stream as usize], p);
            counts[b.stream as usize] += 1;
            n += 1;
        }
    }
    Expected { n_events: n, global_ck, stream_fnv, counts }
}

/// Gate 1: full scan of all segments. Panics on any mismatch.
pub fn verify_log(log_dir: &Path, exp: &Expected, label: &str) {
    let segs = list_segments(log_dir);
    assert!(!segs.is_empty(), "{label}: no segments");
    let mut expect_pos = 0u64;
    let mut events = 0u64;
    let mut global_ck = 0u64;
    let mut stream_fnv = vec![FNV_SEED; STREAMS as usize];
    let mut stream_next = vec![0u64; STREAMS as usize];
    for (i, seg) in segs.iter().enumerate() {
        assert_eq!(seg.id, i as u64, "{label}: segment id gap");
        assert_eq!(seg.base_pos, expect_pos, "{label}: segment base discontinuity");
        let data = fs::read(&seg.path).unwrap();
        let rec = scan(&data, 0, seg.base_pos);
        assert_eq!(rec.stop, StopReason::EndOfLog, "{label}: seg {} stop {:?}", seg.id, rec.stop);
        assert_eq!(rec.safe_offset, data.len() as u64, "{label}: seg {} tail", seg.id);
        for b in &rec.batches {
            let s = b.stream_id as usize;
            assert_eq!(
                b.first_stream_version, stream_next[s],
                "{label}: stream {} version order broken", b.stream_id
            );
            for &(o, l) in &b.payloads {
                let p = &data[o as usize..(o + l as u64) as usize];
                global_ck = global_ck.wrapping_add(p[0] as u64 + p.len() as u64);
                stream_fnv[s] = fnv1a(stream_fnv[s], p);
                stream_next[s] += 1;
                events += 1;
            }
        }
        expect_pos = rec.next_global_pos;
    }
    assert_eq!(events, exp.n_events, "{label}: event count");
    assert_eq!(global_ck, exp.global_ck, "{label}: global checksum");
    for s in 0..STREAMS as usize {
        assert_eq!(stream_next[s], exp.counts[s], "{label}: stream {s} count");
        assert_eq!(stream_fnv[s], exp.stream_fnv[s], "{label}: stream {s} fnv");
    }
}

/// Resolve a pointer list to (count, fnv) by pread from segment files.
pub fn fnv_via_ptrs(files: &HashMap<u64, Arc<File>>, ptrs: &[EventPtr]) -> (u64, u64) {
    let mut fnv = FNV_SEED;
    let mut buf = Vec::new();
    for p in ptrs {
        let f = files
            .get(&p.segment_id)
            .unwrap_or_else(|| panic!("dangling EventPtr to segment {}", p.segment_id));
        buf.resize(p.len as usize, 0);
        f.read_exact_at(&mut buf, p.offset).unwrap();
        fnv = fnv1a(fnv, &buf);
    }
    (ptrs.len() as u64, fnv)
}

/// Sample of streams to verify through the index read path: the hottest ones
/// plus a deterministic spread of the rest.
pub fn sample_streams(exp: &Expected, n: usize) -> Vec<u64> {
    let mut by_count: Vec<u64> = (0..STREAMS).filter(|&s| exp.counts[s as usize] > 0).collect();
    by_count.sort_by_key(|&s| std::cmp::Reverse(exp.counts[s as usize]));
    let hot: Vec<u64> = by_count.iter().take(n / 2).copied().collect();
    let rest: Vec<u64> = by_count
        .iter()
        .skip(n / 2)
        .step_by((by_count.len().saturating_sub(n / 2) / (n / 2)).max(1))
        .take(n / 2)
        .copied()
        .collect();
    hot.into_iter().chain(rest).collect()
}

/// Gate 2: engine index read path returns exact per-stream (count, fnv).
pub fn verify_stream_reads(
    exp: &Expected,
    sample: &[u64],
    label: &str,
    mut read: impl FnMut(u64) -> Option<(u64, u64)>,
) {
    for &s in sample {
        let Some((count, fnv)) = read(s) else { return }; // index-less engine: skip
        assert_eq!(count, exp.counts[s as usize], "{label}: stream {s} read count");
        assert_eq!(fnv, exp.stream_fnv[s as usize], "{label}: stream {s} read fnv");
    }
}
