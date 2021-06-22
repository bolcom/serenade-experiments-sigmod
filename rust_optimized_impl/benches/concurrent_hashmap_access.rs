#[macro_use]
extern crate bencher;
extern crate rand;
extern crate im;
extern crate hashbrown;

use std::collections::HashMap;
use bencher::Bencher;
use std::thread;
use std::sync::Arc;
use rand::Rng;
use im::hashmap::HashMap as ImmutableHashMap;
use hashbrown::hash_map::HashMap as HashbrownHashMap;

benchmark_group!(benches, bench_std_arc, bench_immutablemap_arc, bench_hashbrown_arc);
benchmark_main!(benches);

const NUM_ITEMS: u64 = 1_000_000;

fn bench_std_arc(bench: &mut Bencher) {

    let mut table: HashMap<u64, u64> = HashMap::new();
    for item in 0..NUM_ITEMS {
        table.insert(item, item);
    }

    let table_ref = Arc::new(table);

    bench.iter(|| {
        bencher::black_box(std_arc(table_ref.clone(), 4, 1_000));
    });
}

fn std_arc(table: Arc<HashMap<u64, u64>>, num_threads: usize, num_probes: usize) {
    let threads: Vec<_> =
        (0..num_threads)
            .map(|_| {

                let mytable_ref = table.clone();

                thread::spawn(move || {
                    let mut sum = 0;
                    let mut rng = rand::thread_rng();

                    for _ in 0..num_probes {
                        let key = rng.gen_range(0..NUM_ITEMS);
                        sum += *mytable_ref.get(&key).unwrap();
                    }

                    let _ = sum;
                })
            })
            .collect();

    for handle in threads {
        handle.join().unwrap()
    }
}

fn bench_hashbrown_arc(bench: &mut Bencher) {

    let mut table: HashbrownHashMap<u64, u64> = HashbrownHashMap::new();
    for item in 0..NUM_ITEMS {
        table.insert(item, item);
    }

    let table_ref = Arc::new(table);

    bench.iter(|| {
        bencher::black_box(hashbrown_arc(table_ref.clone(), 4, 1_000));
    });
}

fn hashbrown_arc(table: Arc<HashbrownHashMap<u64, u64>>, num_threads: usize, num_probes: usize) {
    let threads: Vec<_> =
        (0..num_threads)
        .map(|_| {

            let mytable_ref = table.clone();

            thread::spawn(move || {
                let mut sum = 0;
                let mut rng = rand::thread_rng();

                for _ in 0..num_probes {
                    let key = rng.gen_range(0..NUM_ITEMS);
                    sum += *mytable_ref.get(&key).unwrap();
                }

                let _ = sum;
            })
        })
        .collect();

    for handle in threads {
        handle.join().unwrap()
    }
}

fn bench_immutablemap_arc(bench: &mut Bencher) {

    let mut table: HashMap<u64, u64> = HashMap::new();
    for item in 0..NUM_ITEMS {
        table.insert(item, item);
    }

    let table_ref = Arc::new(ImmutableHashMap::from(table));

    bench.iter(|| {
        bencher::black_box(immutablemap_arc(table_ref.clone(), 4, 1_000));
    });
}

fn immutablemap_arc(table: Arc<ImmutableHashMap<u64, u64>>, num_threads: usize, num_probes: usize) {
    let threads: Vec<_> =
        (0..num_threads)
            .map(|_| {

                let mytable_ref = table.clone();

                thread::spawn(move || {
                    let mut sum = 0;
                    let mut rng = rand::thread_rng();

                    for _ in 0..num_probes {
                        let key = rng.gen_range(0..NUM_ITEMS);
                        sum += *mytable_ref.get(&key).unwrap();
                    }

                    let _ = sum;
                })
            })
            .collect();

    for handle in threads {
        handle.join().unwrap()
    }
}