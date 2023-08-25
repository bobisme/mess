use std::ops::Deref;
use std::time::Duration;
use std::{cell::Cell, ops::DerefMut};

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion,
};

use ident::Id;
use mess_db::rocks::db::DB;
use mess_db::write::WriteMessage;
use serde_json::json;

struct SelfDestructingDB(Option<DB>);

impl std::ops::Deref for SelfDestructingDB {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for SelfDestructingDB {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

impl Drop for SelfDestructingDB {
    fn drop(&mut self) {
        let path = self.path().to_owned();
        drop(std::mem::take(&mut self.0));
        ::rocksdb::DB::destroy(&rocksdb::Options::default(), &path).unwrap();
    }
}

fn new_db() -> SelfDestructingDB {
    let path = std::env::temp_dir();
    let path = path.join(Id::new().to_string());
    SelfDestructingDB(Some(DB::new(path).unwrap()))
}

fn write_a_message(db: &DB, expect: Option<u64>) {
    let data = json!({ "one": 1, "two": 2 });
    let metadata = Some(json!({ "three": 3, "four": 4 }));
    let msg = WriteMessage {
        id: Id::new(),
        stream_name: "stream1".into(),
        message_type: "someMsgType".into(),
        data,
        metadata,
        expected_stream_position: expect,
    };
    mess_db::rocks::write::write_mess(db, black_box(msg)).unwrap();
}

pub fn writing_to_disk(c: &mut Criterion) {
    let conn = new_db();
    let mut pos = None;
    c.bench_function("rocks_write_many_messages_to_disk", |b| {
        b.iter(|| {
            write_a_message(&conn, pos);
            pos = pos.and_then(|x| Some(x + 1)).or(Some(0));
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs_f32(3.0));
    targets = writing_to_disk
);
criterion_main!(benches);
