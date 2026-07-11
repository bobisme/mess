//! Loom model for the A2 page-seqlock publication protocol (`--cfg loom`).
//!
//! Tiny by design so it completes: one page with TWO cells, one writer doing
//! a single 2-cell `write_batch` under the page sequence, two concurrent
//! readers doing bounded-retry reads. Asserts:
//!
//! 1. **No torn pair**: any successful read of slot `s` returns exactly
//!    `(1, enc(1, s))` (the pre-populated value) or `(2, enc(2, s))` (the
//!    published value) — never a mix.
//! 2. **Termination via bounded retry**: readers use `try_read` with a fixed
//!    retry budget and are allowed to give up (`None`) while the writer is
//!    mid-publication, so the model has no unbounded spin. A final read after
//!    join must succeed and see the new value.
//!
//! Run: `RUSTFLAGS="--cfg loom" cargo test --release --test loom_model`

#![cfg(loom)]

use loom::sync::Arc;
use loom::thread;
use state_kernel_dense_heads::direct::{CellKind, Page, SeqCell, encode_global};

const RETRY_BUDGET: u32 = 8;

fn reader(page: &Page<SeqCell>, slot: u64) {
    let mut retries = 0u32;
    if let Some((v, g)) =
        SeqCell::try_read(page, slot as usize, RETRY_BUDGET, &mut retries)
    {
        assert!(
            (v == 1 || v == 2) && g == encode_global(v, slot),
            "torn pair: slot={slot} v={v} g={g:#x}"
        );
    }
    // None => bounded retry gave up while the writer held the page odd;
    // that is the documented slow path, and it terminated.
}

#[test]
fn seqlock_no_torn_pair_bounded_retry() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let page = Arc::new(Page::<SeqCell>::with_len(2));
        // Pre-populate both cells at version 1 (single-threaded, pre-spawn).
        SeqCell::write_batch(
            &page,
            &[(0, 1, encode_global(1, 0)), (1, 1, encode_global(1, 1))],
        );

        let w = {
            let page = Arc::clone(&page);
            thread::spawn(move || {
                SeqCell::write_batch(
                    &page,
                    &[(0, 2, encode_global(2, 0)), (1, 2, encode_global(2, 1))],
                );
            })
        };
        let r1 = {
            let page = Arc::clone(&page);
            thread::spawn(move || {
                reader(&page, 0);
                reader(&page, 1);
            })
        };
        let r2 = {
            let page = Arc::clone(&page);
            thread::spawn(move || {
                reader(&page, 1);
                reader(&page, 0);
            })
        };

        w.join().unwrap();
        r1.join().unwrap();
        r2.join().unwrap();

        // Quiescent read must succeed within the budget and see version 2.
        let mut retries = 0u32;
        for slot in 0..2u64 {
            let (v, g) = SeqCell::try_read(&page, slot as usize, RETRY_BUDGET, &mut retries)
                .expect("quiescent bounded read must succeed");
            assert_eq!((v, g), (2, encode_global(2, slot)));
        }
    });
}
