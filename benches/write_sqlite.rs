use std::cell::Cell;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

use async_std::task::block_on;
use criterion::async_executor::AsyncStdExecutor;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId,
    Criterion,
};

use ident::Id;
use serde_json::json;
use sqlx::{SqliteConnection, SqlitePool};

use mess::db::sqlite::write::write_message;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous,
};

async fn new_memory_pool() -> SqlitePool {
    let options = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .pragma("temp_store", "memory")
        .pragma("mmap_size", format!("{}", 10_000_000_000u64));
    SqlitePool::connect_with(options).await.unwrap()
}

struct DiskPool(SqlitePool);

impl Drop for DiskPool {
    fn drop(&mut self) {
        block_on(async {
            self.0.close().await;
        });
        if let Err(err) = std::fs::remove_file("bench.db") {
            eprintln!("could not delete bench.db: {:?}", err);
        }
    }
}

impl Deref for DiskPool {
    type Target = SqlitePool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

async fn new_disk_pool() -> DiskPool {
    let options = SqliteConnectOptions::from_str("sqlite:bench.db?mode=rwc")
        .unwrap()
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .pragma("temp_store", "memory")
        .pragma("mmap_size", format!("{}", 10_000_000_000u64));
    DiskPool(SqlitePool::connect_with(options).await.unwrap())
}

async fn write_a_message(conn: &mut SqliteConnection, expect: i64) {
    let data = json!({ "one": 1, "two": 2 });
    let meta = Some(json!({ "three": 3, "four": 4 }));
    write_message(
        conn,
        black_box(Id::new()),
        black_box("thing-xyz123.twothr"),
        black_box("SomethingHappened"),
        black_box(&data),
        black_box(meta.as_ref()),
        black_box(Some(expect)),
    )
    .await
    .unwrap();
}

pub fn once_to_memory(c: &mut Criterion) {
    c.bench_function("write_message_once_to_memory", |b| {
        b.to_async(AsyncStdExecutor).iter_batched(
            || async {
                let pool = new_memory_pool().await;
                mess::db::sqlite::migration::mig(&pool).await.unwrap();
                pool
            },
            |pool| async {
                let mut conn = pool.await.acquire().await.unwrap();
                write_a_message(&mut conn, 0).await;
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn many_to_memory(c: &mut Criterion) {
    let pool = block_on(async {
        let pool = new_memory_pool().await;
        mess::db::sqlite::migration::mig(&pool).await.unwrap();
        pool
    });
    let pos = Cell::new(0i64);
    c.bench_function("write_many_messages_to_memory", |b| {
        b.to_async(AsyncStdExecutor).iter(|| async {
            let p = pos.get();
            let mut conn = pool.acquire().await.unwrap();
            write_a_message(&mut conn, pos.get()).await;
            pos.set(p + 1);
        })
    });
}

pub fn writing_to_disk(c: &mut Criterion) {
    let pool = block_on(async {
        let pool = new_disk_pool().await;
        mess::db::sqlite::migration::mig(&pool).await.unwrap();
        pool
    });
    let pos = Cell::new(0i64);
    c.bench_function("write_message_to_disk", |b| {
        b.to_async(AsyncStdExecutor).iter(|| async {
            let p = pos.get();
            let mut conn = pool.deref().acquire().await.unwrap();
            write_a_message(&mut conn, pos.get()).await;
            pos.set(p + 1);
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
