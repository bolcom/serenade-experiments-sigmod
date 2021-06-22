extern crate csv;

use std::collections::HashMap;

use itertools::Itertools;
use std::hash::Hasher;

use crate::differential::types::{SessionId, ItemId, SessionItemWithOrder};
use fnv::FnvHasher;
use std::fs::File;
use std::io::LineWriter;
use std::error::Error;

fn csv_reader(file: &str) -> Result<csv::Reader<std::fs::File>, csv::Error> {
    let reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b'\t')
        .from_path(file)?;

    Ok(reader)
}


pub fn create_linewriter_file(outputfilename: &str) -> Result<LineWriter<File>, Box<dyn Error>> {
    let file = File::create(outputfilename)?;
    let file = LineWriter::new(file);
    Ok(file)
}



fn hash_partition(identifier: &u32, num_workers: usize) -> usize {
    let mut hasher: FnvHasher = Default::default();
    hasher.write_u32(*identifier);
    hasher.finish() as usize % num_workers
}

fn interactions_from_csv<'a, R>(
    reader: &'a mut csv::Reader<R>,
    partition: usize,
    num_partitions: usize,
) -> impl Iterator<Item=SessionItemWithOrder> + 'a
    where R: std::io::Read {

    reader.deserialize()
        .filter_map(move |result| {
            if result.is_ok() {
                let raw: (u32, u64, f64) = result.unwrap();
                let (session_id, item_id, order): SessionItemWithOrder =  (raw.0, raw.1, raw.2.round() as u32);

                if hash_partition(&session_id, num_partitions) == partition {
                    Some((session_id, item_id, order))
                } else {
                    println!("hash_partition has no partition match for {}", &session_id);
                    None
                }
            } else {
                println!("result is not ok {}", &result.err().unwrap().to_string());
                None
            }
        })
}

pub fn read_evolving_sessions(evolving_sessions_file: &str) -> HashMap<SessionId, Vec<ItemId>> {
    read_evolving_sessions_partitioned(evolving_sessions_file, 0, 1)
}

pub fn read_evolving_sessions_partitioned(
    evolving_sessions_file: &str,
    partition: usize,
    num_partitions: usize,
) -> HashMap<SessionId, Vec<ItemId>> {
    let mut evolving_sessions_reader = csv_reader(&*evolving_sessions_file).unwrap();
    let evolving_session_interactions: Vec<_> =
        interactions_from_csv(&mut evolving_sessions_reader, partition, num_partitions).collect();

    let evolving_sessions: HashMap<SessionId, Vec<ItemId>> = evolving_session_interactions
        .into_iter()
        .map(|(session_id, item_id, order)| (session_id, (item_id, order)))
        .into_group_map()
        .into_iter()
        .map(|(session_id, mut item_ids_with_order)| {
            item_ids_with_order.sort_unstable_by(|(_, order_a), (_, order_b)| {
                order_a.cmp(order_b)
            });

            let session_items: Vec<ItemId>  = item_ids_with_order.into_iter()
                .map(|(item, _order)| item)
                .collect();

            (session_id, session_items)
        })
        .collect();

    evolving_sessions
}

pub fn read_historical_sessions(historical_sessions_file: &str) -> Vec<(SessionId, ItemId, u32)> {
    read_historical_sessions_partitioned(historical_sessions_file, 0, 1)
}

pub fn read_historical_sessions_partitioned(
    historical_sessions_file: &str,
    partition: usize,
    num_partitions: usize,
) -> Vec<(SessionId, ItemId, u32)> {

    let mut historical_sessions_reader = csv_reader(&*historical_sessions_file).unwrap();
    let historical_sessions: Vec<_> =
        interactions_from_csv(&mut historical_sessions_reader, partition, num_partitions).collect();

    historical_sessions
}