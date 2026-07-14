//! Large-batch producer preparation stays byte-identical across cache reads,
//! fold-chain stamping, live segment roll, and recovery.
#![cfg(not(miri))]

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn batch(seed: u8) -> Vec<RecordToAppend> {
    (0..100)
        .map(|i| {
            let mut data = vec![seed; 250];
            data[0] = i;
            RecordToAppend { message_type: "prepared.event".to_string(), data }
        })
        .collect()
}

#[tokio::test]
async fn prepared_batches_survive_chain_roll_cache_and_reopen() {
    let dir = mess_testkit::sweeping_temp_dir("engine-prepared-chain-roll");
    let path = dir.path().join("store");
    let opts = EngineOptions {
        durability: Durability::Process,
        segment_size: 40 * 1024,
        chain: true,
        ..EngineOptions::default()
    };

    let first = batch(0x11);
    let second = batch(0x22);
    let engine = LogEngine::open_with(&path, opts.clone()).expect("open");
    let a = engine
        .append_batch("stream", Version::NoStream, &first)
        .await
        .expect("first prepared append");
    engine
        .append_batch("stream", a.version, &second)
        .await
        .expect("second prepared append across roll");

    // Live reads hit the write-through capsule adopted from the prepared
    // framed buffer rather than decoding the segment.
    let live = engine
        .read_stream("stream", Version::NoStream, 300)
        .await
        .expect("live read");
    assert_eq!(live.len(), 200);
    for (record, expected) in live.iter().zip(first.iter().chain(&second)) {
        assert_eq!(record.message_type, expected.message_type);
        assert_eq!(record.data, expected.data);
    }
    drop(engine);

    // Reopen validates the on-disk CRC + chain and reads the same bytes from
    // the rolled/head segments, independent of the prior in-memory layout.
    let reopened = LogEngine::open_with(&path, opts).expect("reopen");
    let recovered = reopened
        .read_stream("stream", Version::NoStream, 300)
        .await
        .expect("recovered read");
    assert_eq!(recovered, live);
}
