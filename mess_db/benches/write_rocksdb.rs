use std::borrow::{BorrowMut, Cow};
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};

use ident::Id;
use mess_db::rocks::db::DB;
use mess_db::rocks::write::WriteSerializer;
use mess_db::write::WriteSerialMessage;
use tokio::sync::Mutex;

struct SelfDestructingDB<D: Deref<Target = DB>>(Option<D>);

impl<D> std::ops::Deref for SelfDestructingDB<D>
where
    D: Deref<Target = DB>,
{
    type Target = D;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl<D> Drop for SelfDestructingDB<D>
where
    D: Deref<Target = DB>,
{
    fn drop(&mut self) {
        let path = self.0.as_ref().unwrap().path().to_owned();
        drop(std::mem::take(&mut self.0));
        ::rocksdb::DB::destroy(&rocksdb::Options::default(), path).unwrap();
    }
}

impl<D> SelfDestructingDB<D>
where
    D: Deref<Target = DB> + From<DB>,
{
    fn new() -> Self {
        let path = std::env::temp_dir();
        let path = path.join(Id::new().to_string());
        let db = DB::new(path).unwrap();
        Self(Some(D::from(db)))
    }
}

fn msg_to_write(expect: Option<u64>) -> WriteSerialMessage<'static> {
    let data = b"{ \"one\": 1, \"two\": 2, \"string\": \"Some data here\" }";
    let metadata = b"{ \"three\": 3, \"four\": 4 }";
    mess_db::write::WriteSerialMessage {
        id: Id::new(),
        stream_name: "stream1".into(),
        message_type: "someMsgType".into(),
        data: Cow::Borrowed(data),
        metadata: Cow::Borrowed(metadata),
        expected_position: expect.map(mess_db::StreamPos::Serial),
    }
}

fn write_a_message(db: &DB, expect: Option<u64>, ser: &mut WriteSerializer) {
    let msg = msg_to_write(expect);
    mess_db::rocks::write::write_mess(db, black_box(msg), ser).unwrap();
}

pub fn writing_to_disk(c: &mut Criterion) {
    let conn = SelfDestructingDB::<Box<DB>>::new();
    let mut ser = WriteSerializer::new();
    let mut pos = None;
    c.bench_function("rocks_write_many_messages_to_disk", |b| {
        b.iter(|| {
            write_a_message(&conn, pos, &mut ser);
            pos = pos.map(|x| x + 1).or(Some(0));
        })
    });
}

pub fn writing_to_disk_async(c: &mut Criterion) {
    let conn = SelfDestructingDB::<Arc<DB>>::new();
    let ws: WriteSerializer<1024> = WriteSerializer::new();
    let serial = Arc::new(Mutex::new(ws));
    // let pos = Arc::new(Mutex::new(None));
    let pos = std::sync::atomic::AtomicI64::new(-1);
    c.bench_with_input(
        BenchmarkId::new("rocks_async_write_many_messages_to_disk", 0),
        &(conn, serial, pos),
        |b, (db, ser, pos)| {
            b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(
                || async move {
                    let db = Arc::clone(db);
                    let ser = Arc::clone(ser);
                    let p = match pos.fetch_add(1, Ordering::AcqRel) {
                        x if x < 0 => None,
                        x => Some(x as u64),
                    };
                    // let pos = {
                    //     let pos = Arc::clone(pos);
                    //     let mut guard = pos.lock().await;
                    //     let pos = *guard;
                    //     *guard = pos.map(|x| x + 1).or(Some(0));
                    //     pos
                    // };
                    let msg = msg_to_write(p);
                    let mut guard = ser.lock().await;
                    mess_db::rocks::write::write_mess_async(
                        db,
                        msg,
                        guard.borrow_mut(),
                    )
                    .await
                    .unwrap();
                },
            )
        },
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs_f32(3.0));
    targets = writing_to_disk, writing_to_disk_async
);
criterion_main!(benches);
