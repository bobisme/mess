//! Variants C/D API adapter. Missing product counters remain unavailable.

use mess_store::LogEngine;

use crate::contract;
use crate::schema::InputCounters;
use crate::workload::Workload;

pub const PATH_LABEL_PROCESS: &str = "borrowed";
pub const PATH_LABEL_GROUP: &str = "borrowed";

#[derive(Clone, Copy)]
pub struct InputSnapshot;

pub fn input_snapshot(_engine: &LogEngine) -> InputSnapshot { InputSnapshot }

pub fn input_delta(
    _before: InputSnapshot,
    _after: InputSnapshot,
    workload: Workload,
) -> InputCounters {
    InputCounters {
        waiter_reservations_after: None,
        byte_reservations_after:   None,
        owned_batches:             Some(0),
        owned_records:             Some(0),
        owned_payload_bytes:       Some(0),
        borrowed_batches:          Some(workload.appends()),
        borrowed_records:          Some(workload.events()),
        borrowed_payload_bytes:    Some(workload.payload_total()),
        // These generations do not export neutral defensive-copy counters.
        copied_records:            None,
        copied_bytes:              None,
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
    let registry_batches = match contract::VARIANT {
        "C" => {
            assert_eq!(
                high_water, domain_events,
                "C unexpectedly consumed log positions for metadata"
            );
            0
        }
        "D" => {
            assert_eq!(
                high_water,
                domain_events + fresh_streams + 1,
                "D high-water differs from domain + fresh streams + one type",
            );
            fresh_streams
        }
        variant => panic!("borrowed adapter used by variant {variant}"),
    };
    assert_eq!(metrics.commit.batches, public_appends + registry_batches);
    if group {
        assert_eq!(metrics.commit.groups, 1);
        assert_eq!(metrics.commit.fsync.count, 1);
    } else {
        assert_eq!(metrics.commit.groups, 0);
        assert_eq!(metrics.commit.fsync.count, 0);
    }
}
