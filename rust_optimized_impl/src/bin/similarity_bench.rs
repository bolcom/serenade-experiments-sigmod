#![allow(warnings)]

extern crate serenade_optimized;
extern crate itertools;
extern crate rayon;
extern crate rand_pcg;
extern crate num_format;
extern crate bencher;

use rayon::prelude::*;
use serenade_optimized::vsknn::tree_index::TreeIndex;
use serenade_optimized::vsknn::hashed_index::HashedVSKnnIndex;
use serenade_optimized::vsknn::hashed_index_new::HashIndexNew;
use serenade_optimized::vsknn::hashed_index_var::HashIndexVar;
use serenade_optimized::io;
use num_format::{Locale, ToFormattedString};

use std::collections::HashMap;
use serenade_optimized::vsknn::index::VSKnnIndex;
use serenade_optimized::vsknn::index_new::VSKnnIndexNew;
use std::time::{Instant, Duration};
use rand::{Rng, SeedableRng, thread_rng};
use rand::seq::SliceRandom;
use rand_pcg::Pcg64;
use bencher::black_box;

fn main() {

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
            let hash_index = HashedVSKnnIndex::new(historical_sessions, *m, 1_000_000);
            // let tree_index = TreeIndex::new(&path_train, *m);
            let hnew_index = HashIndexNew::new(&path_train, *m);
            let hvar_index = HashIndexVar::new(&path_train, *m);

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
                    black_box::<Vec<u64>>((0..10_000_000).map(|i| rng_random.gen_range(0..10_000_000)).collect());

                    let duration_array = test_sessions.par_iter()
                        .map(|(key, items)| {
                            let mut rng_length = Pcg64::seed_from_u64(*key as u64);
                            let length = rng_length.gen_range(1..items.len());
                            let session = &items[0..length];
                            let mut duration_sample: Vec<Duration> = vec![Duration::new(0, 0); algorithms.len()];

                            let start = Instant::now();
                            black_box(
                                hvar_index.find_neighbors(&session, *k, *m)
                            );
                            duration_sample[0] = start.elapsed();

                            let start = Instant::now();
                            black_box(
                                hnew_index.find_neighbors(&session, *k, *m)
                            );
                            duration_sample[1] = start.elapsed();

                            let start = Instant::now();
                            black_box(
                                hash_index.find_neighbors(&session, *k, *m)
                            );
                            duration_sample[2] = start.elapsed();

                            // let start = Instant::now();
                            // black_box(
                            //     tree_index.find_neighbors(&session, *k, *m)
                            // );
                            // duration_sample[3] = start.elapsed();

                            duration_sample
                           
                        }).collect::<Vec<_>>();
                                      
                    let mut duration = vec![Duration::new(0, 0); algorithms.len()];
                    
                    for duration_sample in duration_array {
                        for (i, algorithm) in algorithms.iter().enumerate() {
                            duration[i] += duration_sample[i];
                        }
                    }

                    for (i, algorithm) in algorithms.iter().enumerate() {
                        println!("{}\t{}\t{}\t{}\t{}", algorithm, dataset, m, k, (duration[i].as_nanos() as u64 / test_sessions.len()  as u64).to_formatted_string(&Locale::en));
                    }

                }
            }
        }
    }
}