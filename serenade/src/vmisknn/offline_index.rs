use crate::dataframeutils::TrainingDataStats;
use crate::vmisknn::similarity_indexed::SimilarityComputationNew;
use crate::vmisknn::SessionScore;
use crate::vmisknn::SessionTime;
use chrono::NaiveDateTime;
use dary_heap::OctonaryHeap;
use hashbrown::HashMap;
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::BinaryHeap;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::vmisknn::vmisknn_index_noopt::prepare_hashmap;
use crate::vmisknn::vmisknn_index_noopt::read_from_file;
use avro_rs::from_value;
use avro_rs::Reader;
use itertools::Itertools;
use std::sync::{Arc, Mutex};

pub struct ProductAttributes {
    pub(crate) is_adult: bool,
    pub(crate) is_for_sale: bool,
}

pub struct OfflineIndex {
    pub(crate) item_to_top_sessions_ordered: HashMap<u64, Vec<u32>>,
    pub(crate) session_to_max_time_stamp: Vec<u32>,
    pub(crate) item_to_idf_score: HashMap<u64, f64>,
    pub(crate) session_to_items_sorted: Vec<Vec<u64>>,
    pub(crate) training_data_stats: TrainingDataStats,
    pub(crate) item_to_product_attributes: HashMap<u64, ProductAttributes>,
}

impl OfflineIndex {
    pub fn new_from_csv(path_to_training: &str, m_most_recent_sessions: usize) -> Self {
        let start_time = Instant::now();
        println!(
            "reading training data, determine items per training session {}",
            &path_to_training
        );
        let data_train = read_from_file(path_to_training);
        let (
            historical_sessions_train,
            _historical_sessions_id_train,
            historical_sessions_max_time_stamp,
            training_data_stats,
        ) = data_train.unwrap();
        println!(
            "reading training data, determine items per training session:{} micros",
            start_time.elapsed().as_micros()
        );

        let start_time = Instant::now();
        println!("prepare indexes");
        let (
            item_to_top_sessions_ordered,
            item_to_idf_score,
            _session_to_items_sorted,
            item_to_product_attributes,
        ) = prepare_hashmap(
            &historical_sessions_train,
            &historical_sessions_max_time_stamp,
            m_most_recent_sessions,
            training_data_stats.qty_events_p99_5 as usize,
        );
        println!(
            "prepare indexes:{} micros",
            start_time.elapsed().as_micros()
        );

        OfflineIndex {
            item_to_top_sessions_ordered,
            session_to_max_time_stamp: historical_sessions_max_time_stamp,
            item_to_idf_score,
            session_to_items_sorted: historical_sessions_train,
            training_data_stats,
            item_to_product_attributes,
        }
    }

    pub fn new(base_path: &str) -> Self {
        println!(
            "reading training data, determine items per training session {}",
            &base_path
        );
        let start_time = Instant::now();
        let (item_to_top_sessions_ordered, item_to_idf_score, item_to_product_attributes) =
            create_item_indices_from_avro(&*(base_path.to_owned() + "/itemindex/"));
        println!(
            "indexing item indices: {} secs",
            start_time.elapsed().as_secs()
        );
        let start_time = Instant::now();
        let (session_to_items_sorted, session_to_max_time_stamp) =
            create_session_indices_from_avro(&*(base_path.to_owned() + "/sessionindex/"));
        println!(
            "indexing session indices: {} secs",
            start_time.elapsed().as_secs()
        );

        println!("Using hardcoded session duration percentiles.");
        let session_duration_p05 = 14_u64;
        let session_duration_p25 = 77_u64;
        let session_duration_p50 = 248_u64;
        let session_duration_p75 = 681_u64;
        let session_duration_p90 = 1316_u64;
        let session_duration_p95 = 1862_u64;
        let session_duration_p99 = 3359_u64;
        let session_duration_p99_5 = 4087_u64;
        let session_duration_p100 = 539931_u64;

        // Session qty event percentiles:  p5=2 p25=2 p50=3 p75=6 p90=10 p95=14 p99=27 p99.5=34 p100=9408
        println!("Using hardcoded qty event percentiles.");
        let qty_events_p05 = 2_u64;
        let qty_events_p25 = 2_u64;
        let qty_events_p50 = 3_u64;
        let qty_events_p75 = 6_u64;
        let qty_events_p90 = 10_u64;
        let qty_events_p95 = 14_u64;
        let qty_events_p99 = 27_u64;
        let qty_events_p99_5 = 34_u64;
        let qty_events_p100 = 9408_u64;

        let min_time = session_to_max_time_stamp.par_iter().min().unwrap();
        let min_time_date_time = NaiveDateTime::from_timestamp(*min_time as i64, 0);
        let max_time = session_to_max_time_stamp.par_iter().max().unwrap();
        let max_time_date_time = NaiveDateTime::from_timestamp(*max_time as i64, 0);

        let training_data_stats = TrainingDataStats {
            descriptive_name: base_path.to_string(),
            qty_records: session_to_items_sorted.len() * qty_events_p75 as usize,
            qty_unique_session_ids: session_to_items_sorted.len(),
            qty_unique_item_ids: item_to_top_sessions_ordered.len(),
            min_time_date_time,
            max_time_date_time,
            session_duration_p05,
            session_duration_p25,
            session_duration_p50,
            session_duration_p75,
            session_duration_p90,
            session_duration_p95,
            session_duration_p99,
            session_duration_p99_5,
            session_duration_p100,
            qty_events_p05,
            qty_events_p25,
            qty_events_p50,
            qty_events_p75,
            qty_events_p90,
            qty_events_p95,
            qty_events_p99,
            qty_events_p99_5,
            qty_events_p100,
        };

        fn _determine_qty_records_in_avro_files(dir: &str) -> i64 {
            let paths = _dir_to_paths(dir);
            let qty_records = Arc::new(Mutex::new(0_i64));
            paths.par_iter().for_each(|path| {
                let full_path_to_file = path.display().to_string();
                if full_path_to_file.ends_with(".avro") {
                    let file = File::open(&Path::new(&full_path_to_file)).unwrap();
                    let reader = Reader::new(file).unwrap();
                    let qty_records_in_file = reader.into_iter().count();
                    let mut data = qty_records.lock().unwrap();
                    *data += qty_records_in_file as i64;
                }
            });
            let result = qty_records.lock().unwrap().to_owned();
            println!("result: {}", result);
            result
        }
        fn _dir_to_paths(dir_path: &str) -> Vec<PathBuf> {
            fs::read_dir(dir_path)
                .unwrap()
                .map(|file| file.unwrap().path())
                .collect()
        }

        #[allow(non_snake_case)]
        #[derive(Debug, Deserialize)]
        struct ItemIdexAvroSchema {
            ItemId: i64,
            session_indices_time_ordered: Vec<i32>,
            idf: f64,
            ForSale: bool,
            IsAdult: bool,
        }
        fn create_item_indices_from_avro(
            dir: &str,
        ) -> (
            HashMap<u64, Vec<u32>>,
            HashMap<u64, f64>,
            HashMap<u64, ProductAttributes>,
        ) {
            // determine_qty_records_in_avro_files(dir);
            // single threaded: indexing item indices: 161 secs
            let mut item_to_top_sessions_ordered = HashMap::with_capacity(10_000_000);
            let mut item_to_idf = HashMap::with_capacity(10_000_000);
            let mut item_to_product_attributes = HashMap::with_capacity(10_000_000);
            let dir_entry = fs::read_dir(dir).unwrap();
            for path in dir_entry {
                let full_path_to_file = path.unwrap().path().display().to_string();
                if full_path_to_file.ends_with(".avro") {
                    let file = File::open(&Path::new(&full_path_to_file)).unwrap();
                    let reader = Reader::new(file).unwrap();
                    for value in reader {
                        let parse_result = from_value::<ItemIdexAvroSchema>(&value.unwrap());
                        match parse_result {
                            Ok(item_index) => {
                                let top_sessions_ordered = item_index
                                    .session_indices_time_ordered
                                    .iter()
                                    .map(|x| *x as u32)
                                    .collect_vec();
                                item_to_top_sessions_ordered
                                    .insert(item_index.ItemId as u64, top_sessions_ordered);
                                item_to_idf.insert(item_index.ItemId as u64, item_index.idf);
                                let attributes = ProductAttributes {
                                    is_adult: item_index.IsAdult,
                                    is_for_sale: item_index.ForSale,
                                };
                                item_to_product_attributes
                                    .insert(item_index.ItemId as u64, attributes);
                            }
                            Err(err) => {
                                println!("{:?}", err);
                                break;
                            }
                        }
                    }
                }
            }
            println!(
                " item_to_top_sessions_ordered.len():{}",
                item_to_top_sessions_ordered.len()
            );
            (
                item_to_top_sessions_ordered,
                item_to_idf,
                item_to_product_attributes,
            )
        }

        #[allow(non_snake_case)]
        #[derive(Debug, Deserialize)]
        struct SessionIdexAvroSchema {
            SessionIndex: i32,
            item_ids_asc: Vec<i64>,
            Time: i32,
        }
        fn create_session_indices_from_avro(dir: &str) -> (Vec<Vec<u64>>, Vec<u32>) {
            let mut max_used_session_index_position = 0;
            let mut session_to_items_sorted = vec![Vec::new(); 150_000_000];
            let mut timestamps = vec![0; 150_000_000];
            let dir_entry = fs::read_dir(dir).unwrap();
            for path in dir_entry {
                let full_path_to_file = path.unwrap().path().display().to_string();
                if full_path_to_file.ends_with(".avro") {
                    let file = File::open(&Path::new(&full_path_to_file)).unwrap();
                    let reader = Reader::new(file).unwrap();
                    for value in reader {
                        let parse_result = from_value::<SessionIdexAvroSchema>(&value.unwrap());
                        match parse_result {
                            Ok(session_index) => {
                                let session_items_asc = session_index
                                    .item_ids_asc
                                    .iter()
                                    .map(|x| *x as u64)
                                    .collect_vec();
                                let session_id = session_index.SessionIndex as usize;
                                if session_id > max_used_session_index_position {
                                    max_used_session_index_position = session_id;
                                }
                                if session_id >= session_to_items_sorted.len() {
                                    let new_size = session_id + 1;
                                    session_to_items_sorted.resize(new_size, Vec::new());
                                    timestamps.resize(new_size, 0);
                                }
                                session_to_items_sorted[session_id] = session_items_asc;
                                timestamps[session_id] = session_index.Time as u32;
                            }
                            Err(err) => {
                                println!("{:?}", err);
                                break;
                            }
                        }
                    }
                }
            }
            // truncate the vectors if needed. Otherwise we can't find the minimum timestamp in the vector.
            if timestamps.len() > max_used_session_index_position + 1 {
                // we have unused positions in the vectors
                let vector_positions_used = max_used_session_index_position + 1;
                timestamps.truncate(vector_positions_used);
                session_to_items_sorted.truncate(vector_positions_used);
            }
            (session_to_items_sorted, timestamps)
        }

        OfflineIndex {
            item_to_top_sessions_ordered,
            session_to_max_time_stamp,
            item_to_idf_score,
            session_to_items_sorted,
            training_data_stats,
            item_to_product_attributes,
        }
    }
}

impl SimilarityComputationNew for OfflineIndex {
    fn items_for_session(&self, session: &u32) -> &[u64] {
        &self.session_to_items_sorted[*session as usize]
    }

    fn idf(&self, item: &u64) -> f64 {
        self.item_to_idf_score[item]
    }

    fn find_neighbors(
        &self,
        evolving_session: &[u64],
        k: usize,
        m: usize,
    ) -> BinaryHeap<SessionScore> {
        // We use a d-ary heap for the (timestamp, session_id) tuple, a hashmap for the (session_id, score) tuples, and a hashmap for the unique items in the evolving session
        let mut heap_timestamps = OctonaryHeap::<SessionTime>::with_capacity(m);
        let mut session_similarities = HashMap::with_capacity(m);
        let len_evolving_session = evolving_session.len();
        let mut unique = evolving_session.iter().clone().collect_vec();
        unique.sort_unstable();
        unique.dedup();

        let qty_unique_session_items = unique.len() as f64;

        let mut hash_items = HashMap::with_capacity(len_evolving_session);

        //  Loop over items in evolving session in reverse order
        for (pos, item_id) in evolving_session.iter().rev().enumerate() {
            // Duplicate items: only calculate similarity score for the item in the farthest position in the evolving session
            match hash_items.insert(*item_id, pos) {
                Some(_) => {}
                None => {
                    // Find similar sessions in training data
                    if let Some(similar_sessions) = self.item_to_top_sessions_ordered.get(item_id) {
                        let decay_factor =
                            (len_evolving_session - pos) as f64 / qty_unique_session_items;
                        // Loop over all similar sessions.
                        'session_loop: for session_id in similar_sessions {
                            match session_similarities.get_mut(session_id) {
                                Some(similarity) => *similarity += decay_factor,
                                None => {
                                    let session_time_stamp =
                                        self.session_to_max_time_stamp[*session_id as usize];
                                    if session_similarities.len() < m {
                                        session_similarities.insert(*session_id, decay_factor);
                                        heap_timestamps.push(SessionTime::new(
                                            *session_id,
                                            session_time_stamp,

                                        ));
                                    } else {
                                        let mut bottom = heap_timestamps.peek_mut().unwrap();
                                        if session_time_stamp > bottom.time {
                                            // println!("{:?} {:?}", session_time_stamp, bottom.time);
                                            // Remove the the existing minimum time stamp.
                                            session_similarities
                                                .remove_entry(&bottom.session_id);
                                            // Set new minimum timestamp
                                            session_similarities
                                                .insert(*session_id, decay_factor);
                                            *bottom = SessionTime::new(
                                                *session_id,
                                                session_time_stamp,
                                            );
                                        } else {
                                            break 'session_loop;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Return top-k
        let mut closest_neighbors: BinaryHeap<SessionScore> = BinaryHeap::with_capacity(k);
        for (session_id, score) in session_similarities.iter() {
            if closest_neighbors.len() < k {
                let scored_session = SessionScore::new(*session_id, *score);
                closest_neighbors.push(scored_session);
            } else {
                let mut bottom = closest_neighbors.peek_mut().unwrap();
                if score > &bottom.score {
                    let scored_session = SessionScore::new(*session_id, *score);
                    *bottom = scored_session;
                } else if (score - bottom.score).abs() < f64::EPSILON
                    && (self.session_to_max_time_stamp[*session_id as usize]
                        > self.session_to_max_time_stamp[bottom.id as usize])
                {
                    let scored_session = SessionScore::new(*session_id, *score);
                    *bottom = scored_session;
                }
            }
        }
        // Closest neigbours contain unique session_ids and corresponding top-k similarity scores
        closest_neighbors
    }

    fn find_attributes(&self, item_id: &u64) -> Option<&ProductAttributes> {
        self.item_to_product_attributes.get(item_id)
    }
}
