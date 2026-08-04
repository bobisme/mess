//! The read paths: the sealed/hot position resolve behind `read_stream`'s
//! sealed branch, and the `StoredRecord` materialisation both page reads
//! share.
//!
//! Positions come from the sealed tier (the real `ReplaySet`, pointer blocks
//! decoded through the bounded `BlockCache`) unioned with the hot
//! `ActiveIndex` tail, sealed winning the seal-handoff window; bytes come
//! from the block reader's decoded capsules (bn-2ib). Both resolves take a
//! cursor and a limit, so a page costs the page rather than the stream
//! (bn-1u6p).

use super::*;

impl LogEngine {
    /// The sealed-tier position resolve for one stream **page**: the committed
    /// batch entries holding `from_version` or later, across the sealed corpus
    /// (via the real [`ReplaySet`], pointer blocks decoded through the bounded
    /// [`BlockCache`]) **unioned with the hot tail** (batches not yet sealed,
    /// and — since sealed eviction is logical — possibly the same batches again
    /// during handoff; the union dedupes by `first_version`, and the two tiers'
    /// entries for one batch carry the same identity). Version-ascending.
    ///
    /// # Why this takes a cursor and a limit (bn-1u6p)
    ///
    /// The predecessor took only the stream id: it coalesced *every* batch of
    /// the stream across *every* sealed segment into a `BTreeMap`, unioned the
    /// *entire* hot tail, and collected the lot — and `read_stream` then threw
    /// the pre-cursor prefix away. Cost was O(stream depth) per call regardless
    /// of cursor or limit: bn-e93g measured 140 ms of entry resolution around
    /// 0.2 ms of page materialisation at 1.37M events, and a head-positioned
    /// read returning *zero* records still paid 135 ms. Because
    /// [`EventStore::load`](crate::EventStore::load) pages `read_stream` from
    /// zero, cache-miss rehydration of a sealed stream was quadratic (180 s
    /// measured for a 1.37M-event aggregate).
    ///
    /// Both inputs are now bounded and already version-ascending, so the
    /// `BTreeMap` (60 % of the old cost) becomes a two-way merge over at most a
    /// page of entries and the whole-stream collect (24 %) disappears.
    ///
    /// # The dedupe invariant
    ///
    /// **Sealed wins.** During seal handoff a batch exists in both tiers with
    /// the same identity; the old `BTreeMap` insert order encoded that (sealed
    /// inserted first, hot `or_insert`). Here the `Equal` arm of the merge
    /// takes the sealed entry and drops the hot one, which is the same rule
    /// stated directly.
    ///
    /// # Why the merge is bounded too, and why that is safe
    ///
    /// Both sources truncate only when over-full: a short result from either is
    /// the complete remainder of that tier (see
    /// [`ReplaySet::stream_replay_from`] and
    /// [`ActiveIndex::stream_entries_from`]). So the merge can stop as soon as
    /// the output covers `limit` events at or past `from_version` and still be
    /// a **dense, gapless prefix** of the union: it can only run off the end of
    /// one input while the other still has entries if that input was complete.
    pub(super) fn sealed_and_hot_entries_from(
        &self,
        stream_id: u64,
        from_version: u64,
        limit: usize,
    ) -> Result<Vec<StreamEntry>, EngineError> {
        let sealed_segs = self.inner.sealed.segments_for_stream(stream_id);
        let replay = ReplaySet::from_segments(sealed_segs);
        let sealed = replay
            .stream_replay_from(
                stream_id,
                from_version,
                limit,
                &self.inner.block_cache,
            )
            .map_err(|e| EngineError::SealedRead(format!("{e:?}")))?;
        // The hot tail (if the stream also has unsealed batches during
        // handoff), bounded by the same cursor and page.
        let hot = self.inner.active.stream_entries_from(
            stream_id,
            from_version,
            limit,
        );
        Ok(merge_sealed_and_hot(&sealed, &hot, from_version, limit))
    }

    /// Materialise owned [`StoredRecord`]s from `(batch, frame)` picks — the
    /// [`StoredRecord`] compatibility adapter over the block-backed views
    /// (bn-2ib): one book lock resolves every name, then each record copies
    /// its payload slice out of the shared batch arena.
    pub(super) fn materialize(
        &self,
        picks: &[(Arc<DecodedBatch>, usize)],
    ) -> Result<Vec<StoredRecord>, EngineError> {
        let book = self.inner.book.read().expect("book lock");
        let mut out = Vec::with_capacity(picks.len());
        for (batch, k) in picks {
            let stream_name =
                book.stream_name_opt(batch.stream_id).ok_or_else(|| {
                    EngineError::Registry(format!(
                        "read: no interned name for stream_id {}",
                        batch.stream_id
                    ))
                })?;
            let type_id = batch.type_ids[*k];
            let message_type =
                book.type_name_opt(type_id).ok_or_else(|| {
                    EngineError::Registry(format!(
                        "read: no interned name for event_type_id {type_id}"
                    ))
                })?;
            out.push(StoredRecord {
                stream_id:       stream_name.to_string(),
                message_type:    message_type.to_string(),
                data:            batch.payload(*k).to_vec(),
                stream_position: batch.first_stream_version + *k as u64,
                global_position: batch.first_global_pos + *k as u64,
            });
        }
        Ok(out)
    }
}

/// The sealed/hot two-way merge behind
/// [`LogEngine::sealed_and_hot_entries_from`] (bn-1u6p), factored out so the
/// dedupe rule and the bound can be unit-tested directly on constructed
/// entries — at the engine level the two tiers' entries for one batch are
/// byte-identical, so which one wins is otherwise unobservable.
///
/// `sealed` and `hot` are each strictly ascending in `first_version` and each
/// already seeked to `from_version`. The output is their union, ascending,
/// **sealed winning any `first_version` present in both** (the seal-handoff
/// window: sealed eviction is logical, so a just-sealed batch is still in the
/// active index too), truncated as soon as it covers `limit` events at or past
/// `from_version`.
///
/// The truncation is gapless because both inputs share the contract "short
/// implies complete": an input that ran out mid-merge had nothing more to give,
/// so the merge never steps over a hole.
fn merge_sealed_and_hot(
    sealed: &[StreamEntry],
    hot: &[StreamEntry],
    from_version: u64,
    limit: usize,
) -> Vec<StreamEntry> {
    debug_assert!(
        sealed.first().is_none_or(|e| e.last_version() >= from_version)
            && hot.first().is_none_or(|e| e.last_version() >= from_version),
        "both merge inputs must already be seeked to the cursor"
    );
    let mut out: Vec<StreamEntry> =
        Vec::with_capacity(sealed.len().max(hot.len()));
    let (mut i, mut j) = (0usize, 0usize);
    let mut covered = 0usize;
    while covered < limit {
        let pick = match (sealed.get(i), hot.get(j)) {
            (Some(s), Some(h)) => match s.first_version.cmp(&h.first_version) {
                CmpOrdering::Less => {
                    i += 1;
                    *s
                }
                CmpOrdering::Greater => {
                    j += 1;
                    *h
                }
                // Seal handoff: one batch, two tiers. Sealed wins.
                CmpOrdering::Equal => {
                    i += 1;
                    j += 1;
                    *s
                }
            },
            (Some(s), None) => {
                i += 1;
                *s
            }
            (None, Some(h)) => {
                j += 1;
                *h
            }
            (None, None) => break,
        };
        // Events this batch contributes at or past the cursor. Saturating:
        // both inputs are seeked, so a batch entirely below the cursor is a
        // contract violation — it must not panic (nor wrap in release) here.
        let lo = from_version.max(pick.first_version);
        covered += (pick.last_version() + 1).saturating_sub(lo) as usize;
        out.push(pick);
    }
    debug_assert!(
        out.windows(2).all(|w| w[0].first_version < w[1].first_version),
        "sealed/hot merge must be strictly version-ascending"
    );
    out
}

/// bn-1u6p: the sealed/hot bounded merge behind `read_stream`'s sealed branch.
/// These drive [`merge_sealed_and_hot`] on constructed entries because at the
/// engine level the two tiers' entries for one batch are byte-identical — the
/// dedupe RULE is only observable here. The end-to-end proofs (byte-identical
/// pages across an exhaustive cursor sweep, and a real seal-handoff window)
/// live in `tests/engine_sealed_paging.rs`.
#[cfg(test)]
mod sealed_paging_tests {
    use super::*;

    // -- bn-1u6p: the sealed/hot bounded merge ---------------------------

    /// Build a batch entry: `frame_count` events from `first_version`, tagged
    /// with `segment_id`/`offset` so the two tiers' entries for one batch can
    /// be told apart in a test (in production they are identical).
    fn entry(
        first_version: u64,
        frame_count: u32,
        segment_id: u64,
        offset: u64,
    ) -> StreamEntry {
        StreamEntry {
            first_version,
            frame_count,
            first_global_pos: first_version,
            ptr: EventPtr { segment_id, offset },
        }
    }

    /// THE NAMED RISK (bn-1u6p / bn-e93g): during seal handoff a batch lives
    /// in BOTH tiers, and the old `BTreeMap` insert order made the SEALED
    /// entry win. The merge must keep that rule — and deliver the batch
    /// exactly once.
    #[test]
    fn merge_gives_the_handoff_window_to_the_sealed_tier_exactly_once() {
        // Batches 0,10,20,30 sealed; the active index still holds 20,30 (the
        // handoff overlap) plus an unsealed tail at 40,50.
        let sealed = vec![
            entry(0, 10, 7, 100),
            entry(10, 10, 7, 200),
            entry(20, 10, 7, 300),
            entry(30, 10, 7, 400),
        ];
        let hot = vec![
            entry(20, 10, 99, 3000),
            entry(30, 10, 99, 4000),
            entry(40, 10, 99, 5000),
            entry(50, 10, 99, 6000),
        ];
        let got = merge_sealed_and_hot(&sealed, &hot, 0, 1_000);

        let versions: Vec<u64> = got.iter().map(|e| e.first_version).collect();
        assert_eq!(
            versions,
            vec![0, 10, 20, 30, 40, 50],
            "each batch exactly once across the handoff"
        );
        // The overlapping batches carry the SEALED pointers.
        assert_eq!(got[2].ptr, EventPtr { segment_id: 7, offset: 300 });
        assert_eq!(got[3].ptr, EventPtr { segment_id: 7, offset: 400 });
        // The unsealed tail keeps its own.
        assert_eq!(got[4].ptr, EventPtr { segment_id: 99, offset: 5000 });

        // ... and the same holds for a page that STRADDLES the handoff
        // boundary rather than spanning the whole stream: cursor inside the
        // last sealed batch, page running into the hot tail.
        let sealed_seek: Vec<StreamEntry> =
            sealed.iter().copied().filter(|e| e.last_version() >= 35).collect();
        let hot_seek: Vec<StreamEntry> =
            hot.iter().copied().filter(|e| e.last_version() >= 35).collect();
        let got = merge_sealed_and_hot(&sealed_seek, &hot_seek, 35, 20);
        let versions: Vec<u64> = got.iter().map(|e| e.first_version).collect();
        assert_eq!(versions, vec![30, 40, 50]);
        assert_eq!(
            got[0].ptr,
            EventPtr { segment_id: 7, offset: 400 },
            "the straddling handoff batch is still the sealed one"
        );
    }

    /// The merge is a union: neither tier's exclusive entries may be lost, and
    /// the result stays version-ascending whichever side leads.
    #[test]
    fn merge_unions_both_tiers_in_version_order() {
        let sealed = vec![entry(0, 10, 1, 0), entry(20, 10, 1, 1)];
        let hot = vec![entry(10, 10, 2, 0), entry(30, 10, 2, 1)];
        let got = merge_sealed_and_hot(&sealed, &hot, 0, 1_000);
        let versions: Vec<u64> = got.iter().map(|e| e.first_version).collect();
        assert_eq!(versions, vec![0, 10, 20, 30]);
        assert_eq!(got[1].ptr.segment_id, 2);

        // Sealed only, hot only, and both empty.
        assert_eq!(merge_sealed_and_hot(&sealed, &[], 0, 1_000).len(), 2);
        assert_eq!(merge_sealed_and_hot(&[], &hot, 0, 1_000).len(), 2);
        assert!(merge_sealed_and_hot(&[], &[], 0, 1_000).is_empty());
    }

    /// The bound: the merge stops as soon as the output covers `limit` events
    /// at or past the cursor — counting from the CURSOR, not from a straddling
    /// batch's first version — and `limit == 0` materialises nothing.
    #[test]
    fn merge_stops_once_the_page_is_covered() {
        let sealed: Vec<StreamEntry> =
            (0..100u64).map(|i| entry(i * 10, 10, 1, i)).collect();
        let hot: Vec<StreamEntry> = Vec::new();

        assert!(merge_sealed_and_hot(&sealed, &hot, 0, 0).is_empty());
        assert_eq!(merge_sealed_and_hot(&sealed, &hot, 0, 1).len(), 1);
        assert_eq!(merge_sealed_and_hot(&sealed, &hot, 0, 10).len(), 1);
        assert_eq!(merge_sealed_and_hot(&sealed, &hot, 0, 11).len(), 2);
        // Cursor mid-batch: batch [50,59] contributes only 5 events (55..59),
        // so a 10-event page needs the next batch too. (Seeked input, as the
        // engine always passes.)
        let seeked: Vec<StreamEntry> =
            sealed.iter().copied().filter(|e| e.last_version() >= 55).collect();
        let got = merge_sealed_and_hot(&seeked, &hot, 55, 10);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].first_version, 50);
        assert_eq!(got[1].first_version, 60);
        // Never more than the page implies: at most one batch beyond the cut.
        for limit in [1usize, 7, 33, 250] {
            let got = merge_sealed_and_hot(&sealed, &hot, 0, limit);
            assert!(
                got.len() <= limit.div_ceil(10) + 1,
                "limit {limit} resolved {} batches",
                got.len()
            );
        }
    }
}
