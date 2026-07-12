//! Checkpoint fault-injection matrix (design §10.3/§10.4/§10.5):
//! interrupted install at EVERY protocol step, stale anchors (cursor past
//! log end; wrong prefix), wrong page hash, missing page, torn manifest,
//! and interrupted GC at every delete step. Every outcome must be the old
//! valid checkpoint, an older manifest, or a clean fallback — never a
//! wrong state.

use segment_effect::checkpoint::{self, Dir, MemDir};
use segment_effect::hist;
use segment_effect::kernel::KernelState;
use segment_effect::model::{Cursor, Log};
use segment_effect::recover::{
    open_and_fold_suffix, v1_oracle, v2_effects_sequential, RecoverErr,
};

/// Fold the log's first `cut` segments into a fresh kernel state.
fn fold_to(log: &Log, cut: usize) -> KernelState {
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    let upto = if cut == 0 { 0 } else { log.segments[cut - 1].hi };
    st.fold(&log.capsules[..upto]).expect("valid prefix");
    st
}

#[test]
fn interrupted_install_at_every_step_is_safe() {
    let mut total_steps_seen = 0u64;
    for seed in 0..12u64 {
        let h = hist::generate(seed ^ 0xfa11);
        let log = &h.log;
        let oracle = v1_oracle(log).expect("valid");
        let nseg = log.segments.len();
        let cut_a = nseg / 3;
        let cut_b = (2 * nseg) / 3;
        if cut_a == 0 || cut_b <= cut_a {
            continue;
        }

        // Base: checkpoint A durably installed.
        let mut base = MemDir::new();
        let st_a = fold_to(log, cut_a);
        let inst_a =
            checkpoint::install(&mut base, &st_a, None).expect("install A");

        // Dry-run install B on a clone to count its protocol steps.
        let mut st_b = fold_to(log, cut_b);
        // (independent fold; dirty tracking spans everything - full install)
        let mut probe = base.clone();
        let ops_before = probe.ops;
        checkpoint::install(&mut probe, &st_b, Some(&inst_a.manifest))
            .expect("dry-run install B");
        let steps = probe.ops - ops_before;
        assert!(steps >= 8, "install B is at least a few ops");
        total_steps_seen += steps;
        // Sanity: completed install B opens as B.
        let (opened, m) = checkpoint::open(&probe, log).expect("open B");
        assert_eq!(m.cursor, st_b.cursor);
        assert_eq!(opened.cursor, st_b.cursor);

        // Crash at EVERY step k of install B.
        for k in 1..=steps {
            let mut dir = base.clone();
            dir.fail_at = Some(dir.ops + k);
            st_b = fold_to(log, cut_b);
            let r = checkpoint::install(&mut dir, &st_b, Some(&inst_a.manifest));
            assert!(r.is_err(), "seed {seed} step {k}: install must fail");
            dir.crash();
            // §10.4: the old valid checkpoint (A) or a completed-enough B
            // must be chosen; never garbage. And the recovered digest must
            // match the oracle either way.
            let (st, m) = checkpoint::open(&dir, log)
                .expect("A must still be openable after any crash");
            assert!(
                m.cursor == st_a.cursor || m.cursor == st_b.cursor,
                "seed {seed} step {k}: unexpected manifest {:?}",
                m.cursor
            );
            assert_eq!(st.cursor, m.cursor);
            let digest = open_and_fold_suffix(&dir, log).expect("suffix fold");
            assert_eq!(digest, oracle, "seed {seed} step {k}: wrong state");
        }
    }
    assert!(total_steps_seen > 100, "matrix too small");
}

#[test]
fn stale_anchor_cursor_beyond_log_end_is_rejected() {
    for seed in 0..25u64 {
        let h = hist::generate(seed ^ 0x57a1e);
        let log = &h.log;
        let nseg = log.segments.len();
        if nseg < 2 {
            continue;
        }
        // Checkpoint at the FULL log's last boundary...
        let mut dir = MemDir::new();
        let st = fold_to(log, nseg);
        checkpoint::install(&mut dir, &st, None).expect("install");
        // ...then recover against a TRUNCATED log (fewer segments): the
        // manifest's cursor is beyond the recovered end -> reject -> no
        // checkpoint -> effects fallback still matches the truncated oracle.
        let cut = nseg / 2;
        let upto = log.segments[cut - 1].hi;
        let boundaries: Vec<usize> = log.segments[..cut]
            .iter()
            .skip(1)
            .map(|s| s.lo)
            .collect();
        let short = Log::seal(
            log.capsules[..upto].to_vec(),
            &boundaries,
            log.dedupe_span,
        );
        assert!(
            checkpoint::open(&dir, &short).is_none(),
            "seed {seed}: manifest beyond log end must be rejected"
        );
        assert_eq!(
            open_and_fold_suffix(&dir, &short).unwrap_err(),
            RecoverErr::NoCheckpoint
        );
        let d = v2_effects_sequential(&short).expect("effects fallback");
        assert_eq!(d, v1_oracle(&short).expect("oracle"));
    }
}

#[test]
fn wrong_prefix_anchor_is_rejected() {
    for seed in 0..25u64 {
        let h1 = hist::generate(seed ^ 0x0ff);
        let h2 = hist::generate(seed ^ 0xbad);
        let log1 = &h1.log;
        let log2 = &h2.log;
        // Checkpoint from history 1...
        let mut dir = MemDir::new();
        let cut = log1.segments.len() / 2;
        if cut == 0 {
            continue;
        }
        let st = fold_to(log1, cut);
        checkpoint::install(&mut dir, &st, None).expect("install");
        // ...must be ignored when opening history 2 (anchor mismatch or no
        // such boundary), even where cursor indices happen to coincide.
        assert!(
            checkpoint::open(&dir, log2).is_none(),
            "seed {seed}: foreign-prefix manifest must be rejected"
        );
    }
}

#[test]
fn wrong_page_hash_and_missing_page_fall_back() {
    for seed in 0..25u64 {
        let h = hist::generate(seed ^ 0x9a9e);
        let log = &h.log;
        let oracle = v1_oracle(log).expect("valid");
        let nseg = log.segments.len();
        if nseg < 2 {
            continue;
        }
        for missing in [false, true] {
            let mut dir = MemDir::new();
            let st = fold_to(log, nseg / 2);
            let inst = checkpoint::install(&mut dir, &st, None).expect("install");
            // Attack a page file.
            let (_, _, hsh) = inst.manifest.pages[seed as usize % inst.manifest.pages.len()];
            let mut name = String::from("page-");
            for b in hsh {
                name.push_str(&format!("{b:02x}"));
            }
            name.push_str(".page");
            if missing {
                dir.remove(&name).unwrap();
            } else {
                let mut bytes = dir.read(&name).unwrap();
                let mid = bytes.len() / 2;
                bytes[mid] ^= 0x08;
                dir.write_file(&name, &bytes).unwrap();
            }
            // §10.4: manifest rejected; no older manifest -> None; recovery
            // falls back to effects and still matches the oracle.
            assert!(checkpoint::open(&dir, log).is_none());
            let d = v2_effects_sequential(log).expect("effects fallback");
            assert_eq!(d, oracle);
        }
    }
}

#[test]
fn torn_manifest_falls_back_to_older() {
    for seed in 0..25u64 {
        let h = hist::generate(seed ^ 0x70a2);
        let log = &h.log;
        let oracle = v1_oracle(log).expect("valid");
        let nseg = log.segments.len();
        let (ca, cb) = (nseg / 3, (2 * nseg) / 3);
        if ca == 0 || cb <= ca {
            continue;
        }
        let mut dir = MemDir::new();
        let st_a = fold_to(log, ca);
        let ia = checkpoint::install(&mut dir, &st_a, None).expect("A");
        let st_b = fold_to(log, cb);
        let ib = checkpoint::install(&mut dir, &st_b, Some(&ia.manifest))
            .expect("B");
        // Tear manifest B.
        let mut bytes = dir.read(&ib.manifest.name()).unwrap();
        bytes.truncate(bytes.len() / 2);
        dir.write_file(&ib.manifest.name(), &bytes).unwrap();
        let (st, m) = checkpoint::open(&dir, log).expect("A survives");
        assert_eq!(m.cursor, st_a.cursor, "must fall back to A");
        assert_eq!(st.cursor, st_a.cursor);
        let d = open_and_fold_suffix(&dir, log).expect("fold from A");
        assert_eq!(d, oracle);
    }
}

#[test]
fn interrupted_gc_never_kills_retained_manifests() {
    for seed in 0..10u64 {
        let h = hist::generate(seed ^ 0x6c6c);
        let log = &h.log;
        let oracle = v1_oracle(log).expect("valid");
        let nseg = log.segments.len();
        let cuts = [nseg / 4, nseg / 2, (3 * nseg) / 4];
        if cuts[0] == 0 || cuts[1] <= cuts[0] || cuts[2] <= cuts[1] {
            continue;
        }
        // Three checkpoints; GC(retain=2) should drop the oldest manifest
        // and its now-unreachable pages.
        let mut base = MemDir::new();
        let mut manifests = Vec::new();
        let mut cursors: Vec<Cursor> = Vec::new();
        for &c in &cuts {
            let st = fold_to(log, c);
            let inst = checkpoint::install(&mut base, &st, manifests.last())
                .expect("install");
            cursors.push(inst.manifest.cursor);
            manifests.push(inst.manifest);
        }
        // Dry-run GC to count steps.
        let mut probe = base.clone();
        let ops0 = probe.ops;
        checkpoint::gc(&mut probe, 2).expect("gc dry run");
        let steps = probe.ops - ops0;
        assert!(steps >= 2, "gc must have deleted something");
        // Fully-GCed dir still opens the newest and folds correctly.
        let (_, m) = checkpoint::open(&probe, log).expect("open after gc");
        assert_eq!(m.cursor, cursors[2]);
        assert_eq!(open_and_fold_suffix(&probe, log).unwrap(), oracle);

        // Crash GC at every step: the two retained manifests must both
        // remain fully loadable (no reachable page ever deleted).
        for k in 1..=steps {
            let mut dir = base.clone();
            dir.fail_at = Some(dir.ops + k);
            let r = checkpoint::gc(&mut dir, 2);
            assert!(r.is_err(), "seed {seed} gc step {k} must fail");
            dir.crash();
            let (st, m) = checkpoint::open(&dir, log)
                .expect("newest manifest must survive interrupted gc");
            assert_eq!(m.cursor, cursors[2], "newest manifest lost at step {k}");
            assert_eq!(st.cursor, cursors[2]);
            // BOTH retained manifests stay fully loadable: no page
            // reachable from a retained manifest was deleted.
            assert!(
                checkpoint::load_manifest(&dir, &manifests[1], log).is_some(),
                "seed {seed} gc step {k}: second retained manifest broken"
            );
            assert_eq!(open_and_fold_suffix(&dir, log).unwrap(), oracle);
        }
    }
}
