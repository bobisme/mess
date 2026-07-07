//! 4b/4c: randomized crash loop + re-recovery idempotence.
//!
//! Each iteration:
//!   1. plan K random batches, pick a crash spec (byte offset, maybe an
//!      fsync crash, maybe drop/scramble the un-fsynced tail)
//!   2. append until the injected crash fires (or all batches land)
//!   3. materialize the surviving bytes, round-trip them through a tempfile
//!   4. recover; assert:
//!      - every fsync-acknowledged batch is recovered intact and in order
//!      - at most one extra batch is recovered, and only the fully-written
//!        in-flight one (never a partial batch)
//!      - re-recovery is idempotent (same input => same result; truncated
//!        input => same batches, clean end)
//!      - appending after recovery continues correctly (contiguous
//!        global positions, everything recoverable again)

use std::io::{Read, Seek, SeekFrom, Write};

use crash_log::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const ITERATIONS: u64 = 12_000;
const MASTER_SEED: u64 = 0xC0FFEE_D0_5EED;

fn random_events(rng: &mut StdRng) -> Vec<Vec<u8>> {
    let n = rng.gen_range(1..=4);
    (0..n)
        .map(|_| {
            let len = rng.gen_range(0..=64);
            (0..len).map(|_| rng.gen()).collect()
        })
        .collect()
}

#[test]
fn randomized_crash_recovery_loop() {
    let mut acked_total: u64 = 0;
    let mut unacked_surfaced: u64 = 0;
    let mut crashed_iters: u64 = 0;

    for i in 0..ITERATIONS {
        let mut rng = StdRng::seed_from_u64(MASTER_SEED.wrapping_add(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)));

        // Plan the batches up front so we know the full byte extent.
        let k = rng.gen_range(1..=5);
        let planned: Vec<Vec<Vec<u8>>> = (0..k).map(|_| random_events(&mut rng)).collect();
        let total_bytes: usize = planned
            .iter()
            .map(|evs| {
                HEADER_LEN + MARKER_LEN + evs.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum::<usize>()
            })
            .sum();

        // Crash anywhere in the stream; offsets >= total_bytes mean the crash
        // never fires (clean run) unless it lands exactly on the end.
        // A quarter of the time, snap the crash to a batch boundary so the
        // "marker fully written, fsync lost" path gets hammered too.
        let crash_at = if rng.gen_bool(0.25) {
            let boundaries: Vec<usize> = planned
                .iter()
                .scan(0usize, |acc, evs| {
                    *acc += HEADER_LEN
                        + MARKER_LEN
                        + evs.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum::<usize>();
                    Some(*acc)
                })
                .collect();
            boundaries[rng.gen_range(0..boundaries.len())]
        } else {
            rng.gen_range(0..=total_bytes + MARKER_LEN)
        };
        let crash_on_fsync = rng.gen_bool(0.25);
        let drop_unsynced = rng.gen_bool(0.7);
        let scramble = rng.gen_bool(0.3);

        let mut log = Log::new(FaultWriter::new(Some(crash_at), crash_on_fsync));
        let mut acked: Vec<(u64, u64, Vec<Vec<u8>>)> = Vec::new(); // (batch_id, first_pos, events)
        let mut in_flight: Option<(u64, u64, Vec<Vec<u8>>)> = None;

        for evs in &planned {
            let id = log.next_batch_id;
            let pos = log.next_global_pos;
            match log.append_batch(evs) {
                Ok(info) => {
                    assert_eq!(info.batch_id, id);
                    assert_eq!(info.first_global_pos, pos);
                    acked.push((id, pos, evs.clone()));
                }
                Err(Crashed) => {
                    in_flight = Some((id, pos, evs.clone()));
                    crashed_iters += 1;
                    break;
                }
            }
        }
        acked_total += acked.len() as u64;

        // Crash: figure out what survives on disk, then round-trip it
        // through a real tempfile before recovery.
        let surviving = log.disk.crash(&mut rng, drop_unsynced, scramble);
        let mut f = tempfile::tempfile().expect("tempfile");
        f.write_all(&surviving).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        f.read_to_end(&mut data).unwrap();
        assert_eq!(data, surviving);

        let rec = scan(&data, 0, 0);

        // (1) Every fsync-acknowledged batch recovered intact, in order.
        assert!(
            rec.batches.len() >= acked.len(),
            "iter {i}: lost acked batches: recovered {} < acked {} (stop={:?})",
            rec.batches.len(),
            acked.len(),
            rec.stop
        );
        for (r, (id, pos, evs)) in rec.batches.iter().zip(acked.iter()) {
            assert_eq!(r.batch_id, *id, "iter {i}: batch_id mismatch");
            assert_eq!(r.first_global_pos, *pos, "iter {i}: position mismatch");
            let payloads: Vec<&Vec<u8>> = r.events.iter().map(|(_, p)| p).collect();
            let expected: Vec<&Vec<u8>> = evs.iter().collect();
            assert_eq!(payloads, expected, "iter {i}: payload mismatch");
        }

        // (2) No partial batch ever visible: at most one extra batch, and it
        // must be byte-for-byte the fully-written in-flight batch.
        assert!(
            rec.batches.len() <= acked.len() + 1,
            "iter {i}: recovered impossible extra batches"
        );
        if rec.batches.len() == acked.len() + 1 {
            let extra = rec.batches.last().unwrap();
            let (id, pos, evs) = in_flight
                .as_ref()
                .expect("iter {i}: extra recovered batch but nothing was in flight");
            assert_eq!(extra.batch_id, *id, "iter {i}: extra batch is not the in-flight one");
            assert_eq!(extra.first_global_pos, *pos);
            let payloads: Vec<&Vec<u8>> = extra.events.iter().map(|(_, p)| p).collect();
            let expected: Vec<&Vec<u8>> = evs.iter().collect();
            assert_eq!(payloads, expected, "iter {i}: extra batch payload mismatch");
            unacked_surfaced += 1;
        }

        // (4c) Re-recovery idempotence: same bytes => identical result;
        // truncating to safe_offset => same batches and a clean end.
        let rec2 = scan(&data, 0, 0);
        assert_eq!(rec, rec2, "iter {i}: re-recovery not idempotent");
        let truncated = &data[..rec.safe_offset as usize];
        let rec3 = scan(truncated, 0, 0);
        assert_eq!(rec3.batches, rec.batches, "iter {i}: recovery after truncation differs");
        assert_eq!(rec3.safe_offset, rec.safe_offset);
        assert_eq!(rec3.stop, StopReason::EndOfLog);

        // (3) Appending after recovery continues from recovered state.
        let disk2 = FaultWriter::from_recovered(truncated.to_vec());
        let mut log2 = Log::reopen(disk2, &rec);
        let before_pos = rec.next_global_pos;
        let mut appended_events: u64 = 0;
        for _ in 0..2 {
            let evs = random_events(&mut rng);
            appended_events += evs.len() as u64;
            log2.append_batch(&evs).expect("append after recovery must succeed");
        }
        let rec4 = scan(&log2.disk.buf, 0, 0);
        assert_eq!(rec4.stop, StopReason::EndOfLog, "iter {i}: post-recovery log not clean");
        assert_eq!(rec4.batches.len(), rec.batches.len() + 2);
        assert_eq!(rec4.next_global_pos, before_pos + appended_events);
        assert_eq!(rec4.safe_offset, log2.disk.buf.len() as u64);
        // scan() itself enforces global-position contiguity across all batches.
    }

    println!(
        "randomized crash loop: {ITERATIONS} iterations, {crashed_iters} with an injected crash, \
         {acked_total} acked batches verified, {unacked_surfaced} unacked-but-complete batches \
         surfaced (allowed), 0 partial batches visible, 0 acked batches lost"
    );
}
