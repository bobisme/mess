use std::ops::Deref;
use std::ops::DerefMut;
use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion,
};

use ident::Id;
use mess_db::StreamPos;
use rusqlite::Connection;
use serde_json::json;

use mess_db::rusqlite::write::write_message;

fn new_memory_conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    conn.pragma_update(None, "temp_store", "memory").unwrap();
    conn.pragma_update(None, "mmap_size", 10_000_000_000u64).unwrap();
    conn
}

struct DiskConn(Option<Connection>);

impl DiskConn {
    pub fn new(conn: Connection) -> Self {
        Self(Some(conn))
    }
}

impl Drop for DiskConn {
    fn drop(&mut self) {
        let conn = self.0.take();
        if let Some(conn) = conn {
            conn.close().unwrap();
        }
        if let Err(err) = std::fs::remove_file("bench.db") {
            eprintln!("could not delete bench.db: {:?}", err);
        }
    }
}

impl Deref for DiskConn {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl DerefMut for DiskConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

fn new_disk_pool() -> DiskConn {
    let conn = Connection::open("bench.db").unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    conn.pragma_update(None, "temp_store", "memory").unwrap();
    conn.pragma_update(None, "mmap_size", 10_000_000_000u64).unwrap();
    DiskConn::new(conn)
}

fn write_a_message(conn: &Connection, expect: Option<StreamPos>) {
    let data = json!({ "one": 1, "two": 2 });
    let meta = Some(json!({ "three": 3, "four": 4 }));
    write_message(
        conn,
        black_box(Id::new()),
        black_box("thing-xyz123.twothr"),
        black_box("SomethingHappened"),
        black_box(&data),
        black_box(meta.as_ref()),
        black_box(expect),
    )
    .unwrap();
}

pub fn once_to_memory(c: &mut Criterion) {
    c.bench_function("rusq_write_message_once_to_memory", |b| {
        b.iter_batched(
            || {
                let mut conn = new_memory_conn();
                mess_db::rusqlite::migration::migrate(&mut conn).unwrap();
                conn
            },
            |conn| {
                write_a_message(&conn, None);
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn many_to_memory(c: &mut Criterion) {
    let mut conn = new_memory_conn();
    mess_db::rusqlite::migration::migrate(&mut conn).unwrap();
    let mut pos = None;
    c.bench_function("rusq_write_many_messages_to_memory", |b| {
        b.iter(|| {
            write_a_message(&conn, pos);
            pos = Some(match pos {
                Some(x) => x.next(),
                None => StreamPos::Serial(0),
            });
        })
    });
}

pub fn writing_to_disk(c: &mut Criterion) {
    let mut conn = new_disk_pool();
    mess_db::rusqlite::migration::migrate(&mut conn).unwrap();
    let mut pos = None;
    c.bench_function("rusq_write_many_messages_to_disk", |b| {
        b.iter(|| {
            write_a_message(&conn, pos);
            pos = Some(match pos {
                Some(x) => x.next(),
                None => StreamPos::Serial(0),
            });
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs_f32(10.0));
    targets = once_to_memory,
        many_to_memory,
        writing_to_disk
);
criterion_main!(benches);
