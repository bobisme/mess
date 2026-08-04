//! The `Backend` trait implementation for `LogEngine`: the store facade's
//! read and append entry points.
//!
//! The read entry points are why this block lives here (bn-2u64): a trait
//! implementation cannot be split across files, so `head`, `append_batch`
//! and `append_batch_owned` move with `read_stream`, `read_global` and
//! `read_global_page` rather than being separated from them. The read
//! plumbing these methods call is in `super::read`; the append plumbing they
//! call is in the parent module.

use super::*;

impl Backend for LogEngine {
    type Error = EngineError;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        let book = self.inner.book.read().expect("book lock");
        let Some(sid) = book.registry.stream_id(stream_id) else {
            return Ok(Version::NoStream);
        };
        // `$registry`'s head is its version ALLOCATOR, not `heads[0]`: an
        // engine mint bumps the allocator under this lock at submit time and
        // `heads[0]` only at publish time, so a `Registry<LogEngine>` writer
        // reading the head here must see the allocator or its next `expected`
        // would be stale (`bn-2di`).
        if sid == registry::REGISTRY_STREAM_ID {
            return Ok(book.registry_head());
        }
        Ok(book.head(sid))
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let sid = {
            let book = self.inner.book.read().expect("book lock");
            book.registry.stream_id(stream_id)
        };
        let Some(sid) = sid else {
            // Match MockBackend: widen the load→append race window so
            // concurrent writers on one stream genuinely contend.
            tokio::task::yield_now().await;
            return Ok(Vec::new());
        };
        let start = after.next_position();

        // Resolve positions (batch entries), then materialise bytes through
        // the block reader (bn-2ib).
        //
        // Cold path: a stream with sealed batches is served through the real
        // sealed-replay path (`ReplaySet`) unioned with its hot tail.
        // Hot path: a paged clamped slice of the per-stream `ActiveIndex`.
        //
        // bn-1u6p: BOTH branches now seek. The sealed branch used to take
        // neither `start` nor `limit` and resolve the whole stream, making
        // every page of a deep sealed stream cost O(stream depth); it now
        // takes the same two arguments the hot branch always did.
        let sealed = !self.inner.sealed.segments_for_stream(sid).is_empty();
        let entries: Vec<StreamEntry> = if sealed {
            self.sealed_and_hot_entries_from(sid, start, limit)?
        } else {
            self.inner.active.stream_entries_from(sid, start, limit)
        };

        let mut picks: Vec<(Arc<DecodedBatch>, usize)> =
            Vec::with_capacity(limit.min(entries.len() * 2));
        'outer: for e in &entries {
            // Vestigial since bn-1u6p: both resolves now seek, so no entry
            // reaching here ends before the cursor. Kept as a cheap O(1)
            // guard that states the invariant rather than assuming it — it
            // is what used to skip the whole pre-cursor prefix linearly.
            if e.last_version() < start {
                continue;
            }
            let batch = self.inner.reader.batch(
                &self.inner.sealed,
                e.ptr,
                BatchExpect {
                    stream_id:        sid,
                    first_global_pos: e.first_global_pos,
                    frame_count:      e.frame_count,
                    first_version:    Some(e.first_version),
                },
            )?;
            let from = start.max(e.first_version);
            for v in from..=e.last_version() {
                if picks.len() >= limit {
                    break 'outer;
                }
                picks.push((batch.clone(), (v - e.first_version) as usize));
            }
        }
        let page = self.materialize(&picks)?;
        if !sealed {
            // Match MockBackend's contention window on the hot path (the
            // pre-bn-2ib shape: the sealed path returned without yielding).
            tokio::task::yield_now().await;
        }
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        Ok(self.read_global_page(after, limit).await?.records)
    }

    /// The global read, with the scan frontier (`bn-2di`).
    ///
    /// # Why this engine's global sequence has holes
    ///
    /// `$registry` (stream 0) is a real stream in the log: its batches are
    /// committed by the same flat owner and consume global positions like any
    /// other (the accepted cost of landing the registry on v3 rather than
    /// waiting for v4 control capsules). But a registration is engine
    /// bookkeeping, not an application event — delivering `RegistryEventV1`
    /// records into a user's `read_all` or subscription would be a semantic
    /// regression, and every consumer would have to learn to skip them.
    ///
    /// So stream 0 is skipped here, and the positions it holds become holes in
    /// the delivered sequence. `frontier` is what keeps that safe: it reports
    /// how far the scan actually examined, so a subscription can advance its
    /// cursor across a hole without either re-scanning it forever (what
    /// "resume after the last delivered record" would do) or over-shooting a
    /// record the `limit` cut the page short of (what "jump to the watermark"
    /// would do).
    ///
    /// Skipping is free, not merely cheap: a stream-0 entry never reaches
    /// `reader.batch`, so its bytes are not even `pread`.
    async fn read_global_page(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<GlobalPage, Self::Error> {
        // Global positions are dense from 0 up to the published watermark —
        // the same bound the record book's length used to impose. What is NOT
        // dense is the subset of them this method delivers (see above).
        let wm = self.inner.read_watermark.get();
        // `u64::MAX` is the terminal public cursor. Saturation keeps an EOF
        // query at EOF instead of wrapping it back to position 0.
        let start = after.map_or(0, |p| p.saturating_add(1));
        let mut picks: Vec<(Arc<DecodedBatch>, usize)> = Vec::new();
        let mut pos = start;
        // Built lazily: only reads that reach positions the hot index no
        // longer covers (sealed history after a reopen) pay for it. Each
        // entry carries its install generation (the derived-cache key
        // component, review F1/F6), sorted into global A1 order.
        let mut sealed_segs: Option<Vec<(SealedSegmentRef, u64)>> = None;

        while picks.len() < limit && pos < wm {
            // Prefer the hot index while it covers `pos` (in a live process
            // it covers everything ever appended; after a reopen its
            // coverage starts at the first unsealed segment).
            let entries =
                self.inner.active.global_range(pos, limit - picks.len());
            if entries.first().is_some_and(|e| e.first_global_pos <= pos) {
                for e in &entries {
                    if picks.len() >= limit || pos >= wm {
                        break;
                    }
                    if e.first_global_pos > pos {
                        // A coverage hole (a sealed segment between two
                        // hot-served ones): fall through to the sealed tier.
                        break;
                    }
                    if e.stream_id == registry::REGISTRY_STREAM_ID {
                        // Engine bookkeeping: consume the positions, deliver
                        // nothing, and never touch the bytes.
                        pos = e.end_pos().min(wm);
                        continue;
                    }
                    let batch = self.inner.reader.batch(
                        &self.inner.sealed,
                        e.ptr,
                        BatchExpect {
                            stream_id:        e.stream_id,
                            first_global_pos: e.first_global_pos,
                            frame_count:      e.frame_count,
                            first_version:    None,
                        },
                    )?;
                    while pos < e.end_pos() && picks.len() < limit && pos < wm {
                        picks.push((
                            batch.clone(),
                            (pos - e.first_global_pos) as usize,
                        ));
                        pos += 1;
                    }
                }
                continue;
            }

            // Sealed tier: the segment whose contiguous A1 range covers
            // `pos`, walked through its (cached) global batch directory.
            let segs = sealed_segs.get_or_insert_with(|| {
                let mut v = self.inner.sealed.segments_with_gens();
                v.sort_by_key(|(s, _)| (s.base_pos(), s.segment_id()));
                v
            });
            let Some((seg, generation)) = segs
                .iter()
                .find(|(s, _)| {
                    pos >= s.base_pos() && pos < s.base_pos() + s.event_count()
                })
                .cloned()
            else {
                // Below the watermark every position is hot- or cold-served;
                // a gap here means a raced tier handoff — stop the page
                // rather than serve out of order. `frontier` stays at `pos`,
                // so the caller resumes exactly here rather than skipping the
                // unresolved range.
                break;
            };
            let dir = self.inner.reader.global_dir(&seg, generation)?;
            let seg_end = seg.base_pos() + seg.event_count();
            let from = dir.partition_point(|e| e.end_pos() <= pos);
            for e in &dir[from..] {
                if picks.len() >= limit || pos >= wm || pos >= seg_end {
                    break;
                }
                if e.stream_id == registry::REGISTRY_STREAM_ID {
                    pos = e.end_pos().min(wm).min(seg_end);
                    continue;
                }
                let batch = self.inner.reader.batch(
                    &self.inner.sealed,
                    e.ptr,
                    BatchExpect {
                        stream_id:        e.stream_id,
                        first_global_pos: e.first_global_pos,
                        frame_count:      e.frame_count,
                        first_version:    None,
                    },
                )?;
                while pos < e.end_pos() && picks.len() < limit && pos < wm {
                    picks.push((
                        batch.clone(),
                        (pos - e.first_global_pos) as usize,
                    ));
                    pos += 1;
                }
            }
        }
        let records = self.materialize(&picks)?;
        Ok(GlobalPage { records, frontier: pos })
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        // D7: establish in-flight ownership at the API boundary, before even
        // diagnostic counters. This spans registry decoding, record cloning,
        // byte-budget waiting, enqueue, and every early return.
        self.inner.owner.inflight.fetch_add(1, Ordering::AcqRel);
        let inflight = InFlightGuard(Arc::clone(&self.inner.owner.inflight));
        self.inner
            .append_input
            .borrowed_batches
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .append_input
            .borrowed_records
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        // `$registry` (stream 0) is a system stream with its own write path
        // (`bn-2di`, review F2): it takes only `RegistryEventV1` records, each
        // of which must decode and fold cleanly, and it never mints an id from
        // a message type. Routing it here rather than REJECTING it is what
        // keeps spec 04's `Registry<B>` writer — categories, dictionaries,
        // aliases: the record kinds the engine itself never emits — functional
        // against the real engine, through the same `Backend` seam every other
        // stream uses, with `RegistryState` as the single writer AND the single
        // fold. A user appending arbitrary domain frames to the literal name
        // `"$registry"` is still refused (loudly, at the decode), because those
        // frames are the ones recovery interprets as registry records.
        if stream_id == registry::RESERVED_STREAM_NAME {
            return self.append_registry(expected, records, inflight).await;
        }

        let encoded_estimate = HEADER_LEN
            .saturating_add(MARKER_LEN)
            .saturating_add(
                usize::from(self.inner.owner.chain_enabled) * CHAIN_LEN,
            )
            .saturating_add(records.len().saturating_mul(SUBFRAME_HDR_LEN))
            .saturating_add(
                records
                    .iter()
                    .map(|record| record.data.len())
                    .fold(0usize, usize::saturating_add),
            );
        let can_prepare = !records.is_empty()
            && encoded_estimate >= PREPARE_MIN_ENCODED_BYTES
            && (encoded_estimate as u64) <= MAX_BATCH_LEN
            && records.len() <= u32::MAX as usize
            && records
                .iter()
                .all(|record| record.data.len() <= u32::MAX as usize);
        // bn-1gn1: see `enqueue_owned_domain` — the borrowed public path
        // materializes the same prepared copy and is bounded the same way.
        let build_permit = if can_prepare {
            Some(
                self.reserve_owner_bytes(prepare_build_peak(
                    stream_id.len(),
                    records.len(),
                    encoded_estimate,
                    records
                        .iter()
                        .map(|record| record.message_type.len())
                        .fold(0usize, usize::saturating_add),
                ))
                .await?,
            )
        } else {
            None
        };

        let (input, cost) = if !records.is_empty()
            && encoded_estimate >= PREPARE_MIN_ENCODED_BYTES
            // Keep invalid-input error precedence unchanged: the owner first
            // validates `expected`, then its ordinary encoder reports the
            // typed size/count error. Producer preparation is only selected
            // for the common plain shape already known to be representable.
            && (encoded_estimate as u64) <= MAX_BATCH_LEN
            && records.len() <= u32::MAX as usize
            && records.iter().all(|record| record.data.len() <= u32::MAX as usize)
        {
            // Pure producer-side preparation: copy payload bytes directly
            // into their final framed positions while this task can run in
            // parallel with other producers. Every authoritative field is a
            // placeholder; the sole owner resolves type names and the direct
            // writer stamps ids/positions/epoch/chain + covering CRC.
            let mut names: HashMap<&str, u32> = HashMap::new();
            let mut type_names = Vec::new();
            let mut type_slots = Vec::with_capacity(records.len());
            for record in records {
                let name = record.message_type.as_str();
                let slot = if let Some(&slot) = names.get(name) {
                    slot
                } else {
                    let slot =
                        u32::try_from(type_names.len()).map_err(|_| {
                            AppendError::Backend(EngineError::Append(
                                "too many distinct event types in one batch"
                                    .to_string(),
                            ))
                        })?;
                    type_names.push(name.to_owned());
                    names.insert(name, slot);
                    slot
                };
                type_slots.push(slot);
            }
            let subframes: Vec<Subframe<'_>> = records
                .iter()
                .map(|record| Subframe::plain(0, 0, 0, &record.data))
                .collect();
            let zero_chain = [0u8; CHAIN_LEN];
            let batch = PreparedBatch::encode(&BatchInput {
                segment_epoch:        0,
                batch_id:             0,
                first_global_pos:     0,
                stream_id:            0,
                category_id:          0,
                first_stream_version: 0,
                crypto_chain:         self
                    .inner
                    .owner
                    .chain_enabled
                    .then_some(&zero_chain),
                subframes:            &subframes,
            })
            .map_err(|e| {
                AppendError::Backend(EngineError::Append(e.to_string()))
            })?;
            let cost = stream_id
                .len()
                .saturating_add(batch.total_len() as usize)
                .saturating_add(
                    type_names
                        .iter()
                        .map(String::len)
                        .fold(0usize, usize::saturating_add),
                )
                .saturating_add(type_slots.len() * 4);
            (DomainInput::Prepared { type_names, type_slots, batch }, cost)
        } else {
            let copied_bytes = records
                .iter()
                .map(|r| r.message_type.len() + r.data.len())
                .fold(0usize, usize::saturating_add);
            self.inner
                .append_input
                .copied_records
                .fetch_add(records.len() as u64, Ordering::Relaxed);
            self.inner
                .append_input
                .copied_bytes
                .fetch_add(copied_bytes as u64, Ordering::Relaxed);
            // Preserve the borrowed path's baseline accounting exactly. A
            // repeated message-type string remains resident in every cloned
            // record and therefore counts once per record at the admission
            // byte boundary; only the Process-owned path uses compact names.
            let cost = stream_id.len().saturating_add(
                records
                    .iter()
                    .map(|r| r.message_type.len() + r.data.len())
                    .fold(0usize, usize::saturating_add),
            );
            (DomainInput::Records(records.to_vec()), cost)
        };
        let kind = OwnerIntentKind::Domain {
            stream: stream_id.to_owned(),
            expected,
            input,
        };
        match build_permit {
            Some(permit) => {
                self.enqueue_owner_with_permit(kind, cost, permit, inflight)
                    .await
            }
            None => self.enqueue_owner(kind, cost, inflight).await,
        }
    }

    async fn append_batch_owned(
        &self,
        stream_id: &str,
        expected: Version,
        batch: OwnedAppendBatch,
    ) -> Result<Appended, AppendError<Self::Error>> {
        // Attempt 5 admitted ownership transfer for Process but rejected it as
        // a mode-independent optimization. Barriered modes retain the public
        // owned API while deliberately entering the exact borrowed
        // compatibility implementation: the same counters, validation order,
        // preparation threshold, owner admission, and completion path.
        if !matches!(self.inner.owner.durability, Durability::Process) {
            let records = batch.into_records();
            return self.append_batch(stream_id, expected, &records).await;
        }

        // Process is now selected. Establish in-flight ownership before the
        // Process-only input counters and retain it across every early exit.
        self.inner.owner.inflight.fetch_add(1, Ordering::AcqRel);
        let inflight = InFlightGuard(Arc::clone(&self.inner.owner.inflight));
        self.inner.append_input.owned_batches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .append_input
            .owned_records
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.inner
            .append_input
            .owned_payload_bytes
            .fetch_add(batch.payload_bytes() as u64, Ordering::Relaxed);

        if stream_id == registry::RESERVED_STREAM_NAME {
            let records = batch.into_records();
            return self.append_registry(expected, &records, inflight).await;
        }
        self.enqueue_owned_domain(stream_id, expected, batch, inflight).await
    }
}
