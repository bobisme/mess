//! Boring exact reference model: `BTreeMap<(scope, full key), Vec<position>>`
//! plus a `VecDeque` FIFO for window pruning. All candidates must agree with
//! this everywhere (the hard gate).
//!
//! Pruning is memory hygiene only and provably answer-preserving: `w` is
//! monotone in the harness, so a position `< w - span` can never satisfy
//! `p >= w' - span` for any later `w' >= w`.

use std::collections::{BTreeMap, VecDeque};

use crate::{Scope, scoped_key};

pub struct RefModel {
    span: u64,
    map: BTreeMap<Vec<u8>, Vec<u64>>,
    fifo: VecDeque<(u64, Vec<u8>)>,
    last_w: u64,
}

impl RefModel {
    pub fn new(span: u64) -> Self {
        RefModel { span, map: BTreeMap::new(), fifo: VecDeque::new(), last_w: 0 }
    }

    /// Latest live duplicate position at durable end `w`, or None.
    pub fn check(&self, scope: Scope, key: &[u8], w: u64) -> Option<u64> {
        let lo = w.saturating_sub(self.span);
        self.map
            .get(&scoped_key(scope, key))
            .and_then(|ps| ps.last().copied())
            .filter(|&p| p >= lo)
    }

    pub fn insert(&mut self, scope: Scope, key: &[u8], position: u64) {
        let sk = scoped_key(scope, key);
        // Positions strictly increase across the run, so each per-key vec
        // stays ascending.
        self.map.entry(sk.clone()).or_default().push(position);
        self.fifo.push_back((position, sk));
    }

    /// Drop expired positions (answers unchanged; see module doc).
    pub fn prune(&mut self, w: u64) {
        assert!(w >= self.last_w, "reference model requires monotone w");
        self.last_w = w;
        let lo = w.saturating_sub(self.span);
        while let Some((p, _)) = self.fifo.front() {
            if *p >= lo {
                break;
            }
            let (p, sk) = self.fifo.pop_front().unwrap();
            let ps = self.map.get_mut(&sk).expect("fifo/map in sync");
            let i = ps.iter().position(|&x| x == p).expect("position present");
            ps.remove(i);
            if ps.is_empty() {
                self.map.remove(&sk);
            }
        }
    }
}
