//! `bn-e2y`: `mess inspect` surfaces an offline metrics section over a real
//! store, and documents that runtime latency/cache/lag metrics are in-process
//! only.
#![cfg(not(miri))]

use mess_cli::format::{self, Format};
use mess_cli::inspect::{self, InspectOptions};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};
use serde_json::Value;

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_reports_offline_metrics() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-inspect-metrics-inspect-reports-offline-metrics",
    );
    {
        let engine = LogEngine::open(dir.path()).expect("open");
        engine
            .append_batch(
                "acct-1",
                Version::NoStream,
                &[rec("Opened", b"x"), rec("Deposited", b"5")],
            )
            .await
            .unwrap();
        engine
            .append_batch("acct-2", Version::NoStream, &[rec("Opened", b"y")])
            .await
            .unwrap();
        // Drop the engine so the active segment is closed + durable before the
        // separate-process-style read-only inspect scan.
    }

    let report = inspect::run(dir.path(), &InspectOptions::default());
    let json: Value =
        serde_json::from_str(&format::render(&report, Format::Json)).unwrap();

    let m = &json["metrics"];
    assert_eq!(m["scope"], "offline");
    assert_eq!(m["segment_count"], 1, "one active segment on disk");
    assert_eq!(m["active_segment_count"], 1);
    assert_eq!(m["sealed_segment_count"], 0);
    assert_eq!(m["durable_event_count"], 3, "3 events across two streams");
    assert!(
        m["total_size_bytes"].as_u64().unwrap() > 0,
        "segment has bytes on disk"
    );

    // The advisory documenting the in-process/offline split is present.
    let advice = json["advice"].as_array().expect("advice array");
    assert!(
        advice.iter().any(|a| a["type"] == "metrics-scope"),
        "inspect must document that runtime metrics are in-process only: \
         {advice:?}"
    );
}
