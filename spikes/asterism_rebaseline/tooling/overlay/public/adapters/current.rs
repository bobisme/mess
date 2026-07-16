//! Variant A API adapter. Measurement only; it does not change product calls.

use mess_store::LogEngine;

use crate::schema::InputCounters;
use crate::workload::Workload;

pub const PATH_LABEL_PROCESS: &str = "owned";
pub const PATH_LABEL_GROUP: &str = "borrowed-compatible";

#[derive(Clone, Copy)]
pub struct InputSnapshot {
    input:               mess_store::AppendInputMetrics,
    waiter_reservations: u64,
    byte_reservations:   u64,
}

pub fn input_snapshot(engine: &LogEngine) -> InputSnapshot {
    let metrics = engine.metrics();
    InputSnapshot {
        input:               engine.append_input_metrics(),
        waiter_reservations: u64::try_from(metrics.owner_intent_slots_in_use)
            .expect("owner intent slot occupancy exceeds u64"),
        byte_reservations:   u64::try_from(metrics.owner_intent_bytes_in_use)
            .expect("owner intent byte occupancy exceeds u64"),
    }
}

pub fn input_delta(
    before: InputSnapshot,
    after: InputSnapshot,
    workload: Workload,
) -> InputCounters {
    let before_input = before.input;
    let after_input = after.input;
    InputCounters {
        waiter_reservations_after: Some(after.waiter_reservations),
        byte_reservations_after:   Some(after.byte_reservations),
        owned_batches:             Some(
            after_input
                .owned_batches
                .checked_sub(before_input.owned_batches)
                .expect("owned_batches regressed"),
        ),
        owned_records:             Some(
            after_input
                .owned_records
                .checked_sub(before_input.owned_records)
                .expect("owned_records regressed"),
        ),
        owned_payload_bytes:       Some(
            after_input
                .owned_payload_bytes
                .checked_sub(before_input.owned_payload_bytes)
                .expect("owned_payload_bytes regressed"),
        ),
        borrowed_batches:          Some(
            after_input
                .borrowed_batches
                .checked_sub(before_input.borrowed_batches)
                .expect("borrowed_batches regressed"),
        ),
        borrowed_records:          Some(
            after_input
                .borrowed_records
                .checked_sub(before_input.borrowed_records)
                .expect("borrowed_records regressed"),
        ),
        borrowed_payload_bytes:    Some(
            if after_input.borrowed_batches == before_input.borrowed_batches {
                0
            } else {
                workload.payload_total()
            },
        ),
        copied_records:            Some(
            after_input
                .copied_records
                .checked_sub(before_input.copied_records)
                .expect("copied_records regressed"),
        ),
        copied_bytes:              Some(
            after_input
                .copied_bytes
                .checked_sub(before_input.copied_bytes)
                .expect("copied_bytes regressed"),
        ),
    }
}

pub fn assert_oracle_accounting(
    engine: &LogEngine,
    domain_events: u64,
    public_appends: u64,
    fresh_streams: u64,
    group: bool,
) {
    let metrics = engine.metrics();
    let high_water = engine.total_events() as u64;
    assert_eq!(metrics.total_events, high_water);
    assert_eq!(metrics.durable_watermark, high_water);
    assert_eq!(
        high_water,
        domain_events + fresh_streams + 1,
        "v3 high-water differs from domain + fresh streams + one type",
    );
    assert_eq!(metrics.commit.batches, public_appends + fresh_streams);
    if group {
        assert_eq!(metrics.commit.groups, 1);
        assert_eq!(metrics.commit.fsync.count, 1);
    } else {
        assert_eq!(metrics.commit.groups, 0);
        assert_eq!(metrics.commit.fsync.count, 0);
    }
}

pub fn assert_reopen_seed_accounting(
    engine: &LogEngine,
    domain_events: u64,
    fresh_streams: u64,
) {
    let high_water = engine.total_events() as u64;
    assert_eq!(engine.metrics().total_events, high_water);
    assert_eq!(
        high_water,
        domain_events + fresh_streams + 1,
        "v3 reopen seed differs from domain + streams + one shared type",
    );
}
