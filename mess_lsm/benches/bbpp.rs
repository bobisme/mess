use std::sync::Arc;

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
    let bbpp = BBPP::<1024>::new();

    let mut group = c.benchmark_group("BBPP");

    for count in [1, 2 /*, 4, 8, 16*/] {
        group.bench_function(format!("write_{count}"), |b| {
            let mut seed = 0;
            b.iter(|| {
                let mut writer = bbpp.try_writer().unwrap();
                for _ in 0..count {
                    let len = rand_len(&mut seed);
                    writer.push(black_box(&DATA[0..len])).unwrap();
                }
                bbpp.release_writer(writer).unwrap();
            });
        });
    }

    for count in [1, 2, 4] {
        group.bench_function(format!("read_{count}_concurrent"), |b| {
            let bbpp: Arc<BBPP<4_000_000>> = BBPP::new().into();
            let mut seed = 0;
            let mut writer = bbpp.try_writer().unwrap();
            for _ in 0..10_000 {
                let len = rand_len(&mut seed);
                writer.push(black_box(&DATA[0..len])).unwrap();
            }
            bbpp.release_writer(writer).unwrap();
            b.iter(|| {
                let ths = (0..count).map(|_| {
                    let bbpp = Arc::clone(&bbpp);
                    std::thread::spawn(move || {
                        let reader = bbpp.new_reader().unwrap();
                        black_box(reader.iter().count())
                    })
                });
                ths.for_each(|th| assert!(th.join().unwrap() == 10_000));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bbpp_benchmark);
criterion_main!(benches);
