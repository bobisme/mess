use assert2::assert;
use std::{
    sync::{Arc, Barrier},
    time::Instant,
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
        let bbpp = BBPP::<1024>::new();
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

    // for count in [1, 2, availpar] {
    //     group.bench_function(format!("read_at_{count}_concurrent"), |b| {
    //         let bbpp: Arc<BBPP<4_000_000>> = BBPP::new().into();
    //         let first_len;
    //         {
    //             let mut seed = 0;
    //             let mut writer = bbpp.try_writer().unwrap();
    //             let len = rand_len(&mut seed);
    //             writer.push(&DATA[0..len]).unwrap();
    //             first_len = len;
    //             for _ in 0..10_000 {
    //                 let len = rand_len(&mut seed);
    //                 writer.push(&DATA[0..len]).unwrap();
    //             }
    //             bbpp.release_writer(writer).unwrap();
    //         }
    //
    //         b.iter_custom(|iters| {
    //             let start = Instant::now();
    //             for _i in 0..iters {
    //                 let barrier = Arc::new(Barrier::new(count));
    //                 let ths = (0..count).map(|_| {
    //                     let bbpp = Arc::clone(&bbpp);
    //                     let bar = Arc::clone(&barrier);
    //                     std::thread::spawn(move || {
    //                         bar.wait();
    //                         std::thread::sleep(
    //                             core::time::Duration::from_millis(100),
    //                         );
    //                         let reader = bbpp.new_reader().unwrap();
    //                         black_box(reader.read_at(0)).map(|x| x.to_vec());
    //                     })
    //                 });
    //                 ths.for_each(|th| {
    //                     th.join().unwrap();
    //                 });
    //             }
    //             start.elapsed()
    //         });
    //     });
    // }
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

criterion_group!(benches, bbpp_benchmark, read_at_bench);
criterion_main!(benches);
