#![allow(warnings)]

extern crate bencher;
extern crate itertools;
extern crate num_format;
extern crate rand_pcg;
extern crate rayon;
extern crate serenade_optimized;

use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use serenade_optimized::io;
use serenade_optimized::vmisknn::vsknn_index::VSkNNIndex;
use serenade_optimized::vmisknn::vmisknn_index::VMISSkNNIndex;
use serenade_optimized::vmisknn::vmisknn_index_noopt::VMISSkNNIndexNoOpt;
use serenade_optimized::vmisknn::tree_index::TreeIndex;

use bencher::black_box;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng, SeedableRng};
use rand_pcg::Pcg64;
use serenade_optimized::vmisknn::similarity_hashed::SimilarityComputationHash;
use serenade_optimized::vmisknn::similarity_indexed::SimilarityComputationNew;
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn main() {
    // Benchmark with different Rust-based variants of our index
    // and similarity computation to validate the design choices of our index.
    let num_threads = 6;

    println!("index\tdataset\tm\tk\tduration / iter (ns)");

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();

    let algorithms = ["hvar", "hnew", "hash"];

    for dataset in &["1m"] {
        for m in &[500] {
            let path_train = format!("data/private-clicks-{}_train.txt", dataset);
            let path_test = format!("data/private-clicks-{}_test.txt", dataset);

            let historical_sessions = io::read_training_data(&path_train);
            let vsknn_index = VSkNNIndex::new(historical_sessions, *m, 1_000_000);
            // let tree_index = TreeIndex::new(&path_train, *m);
            let vmis_index = VMISSkNNIndex::new(&path_train, *m);
            let vmis_noopt_index = VMISSkNNIndexNoOpt::new(&path_train, *m);

            let mut test_sessions: HashMap<u32, Vec<u64>> = HashMap::new();

            for (session, item, _) in io::read_training_data(&path_test).iter() {
                if !test_sessions.contains_key(session) {
                    test_sessions.insert(*session, Vec::new());
                }

                test_sessions.get_mut(session).unwrap().push(*item);
            }

            let num_repetitions = 3;

            for i in 0..num_repetitions {
                let mut rng_k = Pcg64::seed_from_u64(i);
                let mut topk = [100];
                topk.shuffle(&mut rng_k);

                for k in &topk {
                    let mut rng_random = thread_rng();
                    black_box::<Vec<u64>>(
                        (0..10_000_000)
                            .map(|i| rng_random.gen_range(0..10_000_000))
                            .collect(),
                    );

                    let duration_array = test_sessions
                        .par_iter()
                        .map(|(key, items)| {
                            let mut rng_length = Pcg64::seed_from_u64(*key as u64);
                            let length = rng_length.gen_range(1..items.len());
                            let session = &items[0..length];
                            let mut duration_sample: Vec<Duration> =
                                vec![Duration::new(0, 0); algorithms.len()];

                            let start = Instant::now();
                            black_box(vmis_noopt_index.find_neighbors(&session, *k, *m));
                            duration_sample[0] = start.elapsed();

                            let start = Instant::now();
                            black_box(vmis_index.find_neighbors(&session, *k, *m));
                            duration_sample[1] = start.elapsed();

                            let start = Instant::now();
                            black_box(vsknn_index.find_neighbors(&session, *k, *m));
                            duration_sample[2] = start.elapsed();

                            // let start = Instant::now();
                            // black_box(
                            //     tree_index.find_neighbors(&session, *k, *m)
                            // );
                            // duration_sample[3] = start.elapsed();

                            duration_sample
                        })
                        .collect::<Vec<_>>();

                    let mut duration = vec![Duration::new(0, 0); algorithms.len()];

                    for duration_sample in duration_array {
                        for (i, algorithm) in algorithms.iter().enumerate() {
                            duration[i] += duration_sample[i];
                        }
                    }

                    for (i, algorithm) in algorithms.iter().enumerate() {
                        println!(
                            "{}\t{}\t{}\t{}\t{}",
                            algorithm,
                            dataset,
                            m,
                            k,
                            (duration[i].as_nanos() as u64 / test_sessions.len() as u64)
                                .to_formatted_string(&Locale::en)
                        );
                    }
                }
            }
        }
    }
}
