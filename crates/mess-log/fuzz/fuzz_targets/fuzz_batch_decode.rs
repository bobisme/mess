//! bn-gux: coverage-guided fuzzing of the batch header + subframe decoder,
//! [`mess_log::scanner::decode_batch`].
//!
//! `data` is treated as one candidate batch starting at offset 0 — exactly
//! the shape [`decode_batch`]'s sole production call site
//! ([`scan_batches`](mess_log::scanner), private) hands it: `img` is the
//! remaining unread tail of the segment and `off` is always `< img.len()`.
//! Fuzzing at `off == 0` against the whole (arbitrary, adversarial) `data`
//! buffer covers exactly that contract without inventing an out-of-precondition
//! `off` the real scanner would never pass (see the `pub fn decode_batch` doc
//! comment in `src/scanner.rs` for why `off` is not separately fuzzed).
//!
//! Invariants: header magic/version/flags/A2-length checks, the A3 marker
//! echoes, the A4/A12 mandatory full-batch CRC, and the subframe-tiling walk
//! (`subframes_tile`, whose `compressed_len`-driven pointer arithmetic is the
//! Kani-proof-adjacent surface this target exists to hammer with real bytes)
//! must never panic and never overflow, on ANY input — accept or one of the
//! typed [`mess_log::scanner::ScanStop`] byte-fault variants, nothing else.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_log::scanner::decode_batch;

fuzz_target!(|data: &[u8]| {
    // The return value is intentionally unused beyond forcing evaluation:
    // this target's only assertion is "does not panic" (checked by libFuzzer
    // catching aborts/panics), because every semantic invariant of a
    // successful decode (A2 cap, A3 echoes, A4 CRC, exact subframe tiling)
    // is already enforced *inside* `decode_batch` itself — an `Ok(Decoded)`
    // it returns is by construction a batch that passed every one of those
    // checks against these exact bytes.
    let _ = decode_batch(data, 0);
});
