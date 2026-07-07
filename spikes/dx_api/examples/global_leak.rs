//! Empirical demonstration of a mess_db bug found during this spike:
//! `CACHED_GLOBAL` in `mess_db/src/rocks/write.rs` is a process-wide
//! `static mut AtomicU64`, so the "last global position" cache leaks across
//! independent DB instances in the same process. A brand-new, empty database
//! starts its global positions after whatever another database in the same
//! process last wrote.
//!
//! Run: `cargo run --release --example global_leak`

use dx_api::store::{EventStore, Version};
use dx_api::{Aggregate, CodecError, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Ping {
    Ping,
}

impl Event for Ping {
    fn name(&self) -> &'static str {
        "ping"
    }
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(b"{}".to_vec())
    }
    fn decode(_: &str, _: &[u8]) -> Result<Self, CodecError> {
        Ok(Ping::Ping)
    }
}

#[derive(Debug, Default)]
struct Nothing;

impl Aggregate for Nothing {
    type Event = Ping;
    fn apply(&mut self, _: &Ping) {}
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let base =
        std::env::temp_dir().join(format!("dxleak{}", std::process::id()));
    let p1 = base.join("db1");
    let p2 = base.join("db2");
    std::fs::create_dir_all(&p1).unwrap();
    std::fs::create_dir_all(&p2).unwrap();

    let s1 = EventStore::open(&p1).unwrap();
    for _ in 0..5 {
        let loaded = s1.load::<Nothing>("s").await.unwrap();
        s1.append("s", loaded.version, &[Ping::Ping]).await.unwrap();
    }
    let c1 = {
        let loaded = s1.load::<Nothing>("s").await.unwrap();
        s1.append("s", loaded.version, &[Ping::Ping]).await.unwrap()
    };
    println!("db1 6th append: global position = {:?}", c1.last_global_position);

    // A completely separate, empty database in the same process:
    let s2 = EventStore::open(&p2).unwrap();
    let c2 = s2.append("s", Version::NoStream, &[Ping::Ping]).await.unwrap();
    println!(
        "db2 (fresh, empty db) FIRST append: global position = {:?} \
         (expected 1)",
        c2.last_global_position
    );

    let _ = std::fs::remove_dir_all(&base);
}
