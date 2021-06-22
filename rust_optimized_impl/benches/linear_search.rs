#[macro_use]
extern crate bencher;
extern crate rand;

use bencher::Bencher;
use rand::Rng;
use itertools::Itertools;
use std::collections::HashSet as HashSetStd;
use hashbrown::HashSet as HashSetBrown;

benchmark_group!(benches, bench_linear_search, bench_hash_std, bench_hash_brown);
benchmark_main!(benches);

const NUM_ITEMS_IN_EVOLVING_SESSION: usize = 10;
const MAX_ITEM_ID: u64 = 2_2278_380;

const P99_NUM_ITEMS_IN_HISTORY_SESSION: usize = 38;
const NUM_HISTORY_SESSIONS: usize = 500;

/**
* A comparison between hashbrown and a linear search.
*
*/

fn bench_linear_search(bench: &mut Bencher) {
    let mut rng = rand::thread_rng();
    let floor = rng.gen_range(9200000000000000..9200000000000001);
    let evolving_session: Vec<u64> = (0..NUM_ITEMS_IN_EVOLVING_SESSION)
        .map(|_| rng.gen_range(0..MAX_ITEM_ID) + floor)
        .sorted()
        .unique()
        .collect();

    let weights: Vec<f64> = (0..evolving_session.len()).map(|_| rng.gen_range(0..10) as f64 / 10.0).collect();

    let historical_sessions: Vec<Vec<u64>> = (0..NUM_HISTORY_SESSIONS)
        .map(|_| {
            let session_length = rng.gen_range(1..P99_NUM_ITEMS_IN_HISTORY_SESSION);
            let mut history_session = Vec::with_capacity(session_length);
            for i in 0..session_length {
                history_session.push(rng.gen_range(0..MAX_ITEM_ID));
            }
            history_session.sort();
            history_session.dedup();
            history_session
        })
        .collect();

    bench.iter(|| {
        bencher::black_box(do_linear_search(&evolving_session, &weights, &historical_sessions));
    });
}


fn do_linear_search(sorted_evolving_session: &Vec<u64>, position_weights: &Vec<f64>, sorted_history_sessions: &Vec<Vec<u64>>) -> Vec<f64> {
    let mut similarities = Vec::with_capacity(sorted_history_sessions.len());
    sorted_history_sessions.iter().for_each(|sorted_history_session| {
        let mut similarity = 0_f64;
        let mut i: usize = 0;
        let mut j: usize = 0;
        while i < sorted_evolving_session.len() && j < sorted_history_session.len() {
            if &sorted_evolving_session[i] == &sorted_history_session[j] {
                similarity = similarity + &position_weights[i];
                i = i + 1;
                j = j + 1;
            } else if &sorted_evolving_session[i] < &sorted_history_session[j] {
                i = i + 1;
            } else {
                j = j + 1;
            }
        }
        similarities.push(similarity);
    });
    similarities
}



fn do_hash_std(sorted_evolving_session: &Vec<u64>, position_weights: &Vec<f64>, sorted_history_sessions: &Vec<HashSetStd<u64>>) -> Vec<f64> {
    let mut similarities = Vec::with_capacity(sorted_history_sessions.len());
    sorted_history_sessions.iter().for_each(|sorted_history_session| {
        let mut similarity = 0_f64;
        for (pos, item_id) in sorted_evolving_session.iter().enumerate() {
            if sorted_history_session.contains(&item_id) {
                similarity = similarity + position_weights[pos];
            }
        }
        similarities.push(similarity);
    });
    similarities
}



fn bench_hash_std(bench: &mut Bencher) {
    let mut rng = rand::thread_rng();
    let floor = rng.gen_range(9200000000000000..9200000000000001);
    let evolving_session: Vec<u64> = (0..NUM_ITEMS_IN_EVOLVING_SESSION)
        .map(|_| rng.gen_range(0..MAX_ITEM_ID) + floor)
        .sorted()
        .unique()
        .collect();

    let weights: Vec<f64> = (0..evolving_session.len()).map(|_| rng.gen_range(0..10) as f64 / 10.0).collect();

    let historical_sessions: Vec<HashSetStd<u64>> = (0..NUM_HISTORY_SESSIONS)
        .map(|_| {
            let session_length = rng.gen_range(1..P99_NUM_ITEMS_IN_HISTORY_SESSION);
            let mut history_session = HashSetStd::new();
            for _ in 0..session_length {
                history_session.insert(rng.gen_range(0..MAX_ITEM_ID));
            }
            history_session
        })
        .collect();

    bench.iter(|| {
        bencher::black_box(do_hash_std(&evolving_session, &weights, &historical_sessions));
    });
}



fn do_hash_brown(sorted_evolving_session: &Vec<u64>, position_weights: &Vec<f64>, sorted_history_sessions: &Vec<HashSetBrown<u64>>) -> Vec<f64> {
    let mut similarities = Vec::with_capacity(sorted_history_sessions.len());
    sorted_history_sessions.iter().for_each(|sorted_history_session| {
        let mut similarity = 0_f64;
        for (pos, item_id) in sorted_evolving_session.iter().enumerate() {
            if sorted_history_session.contains(&item_id) {
                similarity = similarity + position_weights[pos];
            }
        }
        similarities.push(similarity);
    });
    similarities
}


fn bench_hash_brown(bench: &mut Bencher) {
    let mut rng = rand::thread_rng();
    let floor = rng.gen_range(9200000000000000..9200000000000001);
    let evolving_session: Vec<u64> = (0..NUM_ITEMS_IN_EVOLVING_SESSION)
        .map(|_| rng.gen_range(0..MAX_ITEM_ID) + floor)
        .sorted()
        .unique()
        .collect();

    let weights: Vec<f64> = (0..evolving_session.len()).map(|_| rng.gen_range(0..10) as f64 / 10.0).collect();

    let historical_sessions: Vec<HashSetBrown<u64>> = (0..NUM_HISTORY_SESSIONS)
        .map(|_| {
            let session_length = rng.gen_range(1..P99_NUM_ITEMS_IN_HISTORY_SESSION);
            let mut history_session = HashSetBrown::new();
            for _ in 0..session_length {
                history_session.insert(rng.gen_range(0..MAX_ITEM_ID));
            }
            history_session
        })
        .collect();

    bench.iter(|| {
        bencher::black_box(do_hash_brown(&evolving_session, &weights, &historical_sessions));
    });
}
