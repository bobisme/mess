use assert2::assert;
use crossbeam_utils::atomic::AtomicConsume;
use std::{
    sync::{atomic::Ordering, mpsc::channel, Arc, Barrier},
    time::{Duration, Instant},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use mess_lsm::bbpp::BBPP;

const DATA_SIZE: usize = 1_024;
const DATA: [u8; DATA_SIZE] = {
    let mut data = [0u8; DATA_SIZE];
    let mut i = 0usize;
    while i < DATA_SIZE {
        data[i] = (i % 256) as u8;
        i += 1;
    }
    data
};

fn rand_len(seed: &mut u64) -> usize {
    ((wyhash::wyrng(seed) & 0xFF) + 100) as usize
}

fn rand_loop_count(seed: &mut u64) -> usize {
    (wyhash::wyrng(seed) & 0x0F) as usize
}

pub fn bbpp_benchmark(c: &mut Criterion) {
    let availpar = std::thread::available_parallelism().unwrap().get();

    let mut group = c.benchmark_group("BBPP");

    group.bench_function("push", |b| {
        let bbpp = BBPP::<2048>::with_full_capacity();
        let mut seed = 0;
        b.iter(|| {
            let mut writer = bbpp.try_writer().unwrap();
            let len = rand_len(&mut seed);
            writer.push(black_box(&DATA[0..len])).unwrap();
            bbpp.release_writer(writer).unwrap();
        });
    });

    for n_threads in [1, 2, availpar] {
        group.bench_function(format!("push_{n_threads}_threads"), move |b| {
            let mut seed = 0;
            b.iter(|| {
                let bbpp = Arc::new(BBPP::<1024>::new());
                let ths: Vec<_> = (0..n_threads)
                    .map(|_| {
                        let bbpp = Arc::clone(&bbpp);
                        std::thread::spawn(move || {
                            let mut writer = loop {
                                if let Some(writer) = bbpp.try_writer() {
                                    break writer;
                                };
                                std::hint::spin_loop();
                            };
                            let len = rand_len(&mut seed);
                            writer.push(black_box(&DATA[0..len])).unwrap();
                            bbpp.release_writer(writer).unwrap();
                        })
                    })
                    .collect();
                for th in ths {
                    th.join().unwrap();
                }
            });
        });
    }

    for n_entries in [1_000, 10_000, 100_000] {
        group.bench_function(
            format!("iter_{n_entries}_entries_main_thread"),
            |b| {
                let bbpp: Arc<BBPP<40_000_000>> = BBPP::new().into();
                let instant = Instant::now();
                let mut writer = bbpp.try_writer().unwrap();
                let mut seed = instant.elapsed().as_nanos() as u64;
                for _ in 0..n_entries {
                    let len = rand_len(&mut seed);
                    let popped = writer.push(&DATA[0..len]).unwrap();
                    assert!(popped.len() == 0);
                }
                bbpp.release_writer(writer).unwrap();

                b.iter(|| {
                    let reader = bbpp.new_reader().unwrap();
                    black_box(reader.iter().count())
                });
            },
        );
    }

    for n_threads in [1, 2, availpar] {
        for n_entries in [1_000, 10_000, 100_000] {
            group.bench_function(
                format!("iter_{n_entries}_entries_{n_threads}_threads"),
                |b| {
                    let bbpp: Arc<BBPP<40_000_000>> = BBPP::new().into();
                    let instant = Instant::now();
                    let mut writer = bbpp.try_writer().unwrap();
                    let mut seed = instant.elapsed().as_nanos() as u64;
                    for _ in 0..n_entries {
                        let len = rand_len(&mut seed);
                        let popped = writer.push(&DATA[0..len]).unwrap();
                        assert!(popped.len() == 0);
                    }
                    bbpp.release_writer(writer).unwrap();

                    b.iter(|| {
                        let ths: Vec<_> = (0..n_threads)
                            .map(|_| {
                                let bbpp = Arc::clone(&bbpp);
                                std::thread::spawn(move || {
                                    let reader = bbpp.new_reader().unwrap();
                                    black_box(reader.iter().count())
                                })
                            })
                            .collect();
                        for th in ths {
                            let result = th.join().unwrap();
                            assert!(result == n_entries);
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

pub fn read_at_bench(c: &mut Criterion) {
    let availpar = std::thread::available_parallelism().unwrap().get();
    let mut group = c.benchmark_group("BBPP_read_at");

    for n_threads in [1, 2, availpar] {
        group.bench_function(
            format!("read_at_{n_threads}_concurrent").as_str(),
            move |b| {
                let bbpp: Arc<BBPP<4_000_000>> = BBPP::new().into();
                {
                    let mut seed = 0;
                    let mut writer = bbpp.try_writer().unwrap();
                    let len = rand_len(&mut seed);
                    writer.push(&DATA[0..len]).unwrap();
                    // first_len = len;
                    for _ in 0..10_000 {
                        let len = rand_len(&mut seed);
                        writer.push(&DATA[0..len]).unwrap();
                    }
                    bbpp.release_writer(writer).unwrap();
                }

                b.iter(|| {
                    let ths: Vec<_> = (0..n_threads)
                        .map(|_| {
                            let bbpp = Arc::clone(&bbpp);
                            std::thread::spawn(move || {
                                let reader = bbpp.new_reader().unwrap();
                                black_box(reader.read_at(0))
                                    .map(|x| x.to_vec());
                            })
                        })
                        .collect();
                    for th in ths {
                        th.join().unwrap();
                    }
                });
            },
        );
    }
}

#[derive(Debug)]
enum Op {
    Iterate { count: usize },
    Write { bytes: usize },
}

pub fn under_read_write_contention(c: &mut Criterion) {
    let availpar = std::thread::available_parallelism().unwrap().get();
    let mut group = c.benchmark_group("BBPP_contention");

    for n_threads in [1, 2, availpar] {
        group.bench_function(
            format!("contention_1_writer_{n_threads}_readers").as_str(),
            move |b| {
                let bbpp: Arc<BBPP<4_000_000>> = BBPP::new().into();
                {
                    let mut seed = 0;
                    let mut writer = bbpp.try_writer().unwrap();
                    let len = rand_len(&mut seed);
                    writer.push(&DATA[0..len]).unwrap();
                    for _ in 0..1_000 {
                        let len = rand_len(&mut seed);
                        writer.push(&DATA[0..len]).unwrap();
                    }
                    bbpp.release_writer(writer).unwrap();
                }

                b.iter_custom(|iters| {
                    let nanos = Arc::new(std::sync::atomic::AtomicU64::new(0));
                    let (send, recv) = channel();
                    for _ in 0..iters {
                        // readers
                        let mut ths: Vec<_> = (0..n_threads)
                            .map(|_| {
                                let bbpp = Arc::clone(&bbpp);
                                let nanos = Arc::clone(&nanos);
                                let send = send.clone();
                                std::thread::spawn(move || {
                                    let start = Instant::now();
                                    for _ in 0..10 {
                                        let reader = bbpp.new_reader().unwrap();
                                        let count =
                                            black_box(reader.iter().count());
                                        send.send(Op::Iterate { count })
                                            .unwrap();
                                        std::thread::sleep(
                                            Duration::from_micros(100),
                                        );
                                    }
                                    nanos.fetch_add(
                                        start.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                })
                            })
                            .collect();
                        // writers
                        {
                            let mut seed = 0;
                            let bbpp = Arc::clone(&bbpp);
                            let nanos = Arc::clone(&nanos);
                            let send = send.clone();
                            ths.push(std::thread::spawn(move || {
                                for _ in 0..10 {
                                    let start = Instant::now();
                                    let mut writer = loop {
                                        if let Some(writer) = bbpp.try_writer()
                                        {
                                            break writer;
                                        };
                                        std::hint::spin_loop();
                                    };
                                    for _ in 0..10 {
                                        let len = rand_len(&mut seed);
                                        writer
                                            .push(black_box(&DATA[0..len]))
                                            .unwrap();
                                        send.send(Op::Write { bytes: len })
                                            .unwrap();
                                    }
                                    bbpp.release_writer(writer).unwrap();
                                    nanos.fetch_add(
                                        start.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                    std::thread::sleep(Duration::from_micros(
                                        100,
                                    ));
                                }
                            }));
                        }
                        for th in ths {
                            th.join().unwrap();
                        }
                    }
                    drop(send);
                    let mut total_iters = 0;
                    let mut total_iter_count = 0;
                    let mut total_writes = 0;
                    let mut total_writes_bytes = 0;
                    for op in recv {
                        match op {
                            Op::Iterate { count } => {
                                total_iters += 1;
                                total_iter_count += count;
                            }
                            Op::Write { bytes } => {
                                total_writes += 1;
                                total_writes_bytes += bytes;
                            }
                        }
                    }
                    // println!(
                    //     r#"
                    // bench_iters={iters},
                    // total_iters={total_iters},
                    // total_iter_count={total_iter_count},
                    // total_writes={total_writes},
                    // total_writes_bytes={total_writes_bytes}"#
                    // );
                    Duration::from_nanos(nanos.load_consume())
                });
            },
        );
    }
}

criterion_group!(
    benches,
    bbpp_benchmark,
    read_at_bench,
    under_read_write_contention
);
criterion_main!(benches);
