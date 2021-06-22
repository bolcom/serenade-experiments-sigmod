extern crate differential_dataflow;
extern crate serenade;
extern crate itertools;
extern crate fnv;

use std::collections::HashMap;

use std::io::prelude::*;
use std::io::BufWriter;
use std::fs::File;
use std::path::Path;

use itertools::Itertools;

use differential_dataflow::input::InputSession;

use serenade::io;
use serenade::differential::{vsknn, update_recommendations};
use serenade::differential::types::{SessionId, ItemId, OrderedSessionItem, Order, Similarity, ScoredItem};


fn main() {
    let historical_sessions_file = std::env::args().nth(2)
        .expect("Historical sessions file not specified!");

    let evolving_sessions_file = std::env::args().nth(3)
        .expect("Evolving sessions file not specified!");

    let predictions_output_file = std::env::args().nth(4)
        .expect("Predictions output file not specified!");

    let latencies_output_file = std::env::args().nth(5)
        .expect("Latencies output file not specified!");

    let k: usize = std::env::args().nth(6)
        .expect("Number of neighbors not specified!").parse()
        .expect("Cannot parse number of neighbors");

    let m: usize = std::env::args().nth(7)
        .expect("Sample size not specified!").parse()
        .expect("Cannot parse sample size");

    let historical_sessions = io::read_historical_sessions(&*historical_sessions_file);
    let num_historical_sessions: usize = historical_sessions.len();

    timely::execute_from_args(std::env::args(), move |worker| {
        let historical_sessions = io::read_historical_sessions_partitioned(
            &*historical_sessions_file,
            worker.index(),
            worker.peers(),
        );

        let evolving_sessions = io::read_evolving_sessions_partitioned(
            &*evolving_sessions_file,
            worker.index(),
            worker.peers(),
        );

        eprintln!("Found {} interactions in historical sessions", historical_sessions.len());

        let mut historical_sessions_input: InputSession<_, OrderedSessionItem, _> =
            InputSession::new();
        let mut evolving_sessions_input: InputSession<_, (SessionId, ItemId), _> =
            InputSession::new();

        let (probe, mut trace) = vsknn(
            worker,
            &mut historical_sessions_input,
            &mut evolving_sessions_input,
            k,
            m,
            num_historical_sessions,
        );

        let mut predictions_writer = io::create_linewriter_file(&*predictions_output_file).unwrap();
        let mut latency_writer = io::create_linewriter_file(&*latencies_output_file).unwrap();
        latency_writer.write(("position,latency_in_micros\n").as_ref()).unwrap();

        eprintln!("Loading {} historical interactions", historical_sessions.len());

        for (session, item, order) in &historical_sessions {
            historical_sessions_input.insert((*session, (*item, Order::new(*order))));
        }

        historical_sessions_input.close();

        let mut recommmendations: HashMap<SessionId, HashMap<ItemId, f64>> = HashMap::new();
        let mut time = 0;

        let session_ids: Vec<_> = evolving_sessions.keys().sorted().collect();

        let path = format!("vsknn_differential_predictions-{}.txt", worker.index());
        let mut _output_file = BufWriter::new(File::create(&Path::new(&path)).unwrap());

        for evolving_session_id in session_ids {
            let evolving_session_items = &evolving_sessions[evolving_session_id];
            if cfg!(debug_assertions) {
                eprintln!(
                    "\nProcessing session {} with {} items",
                    evolving_session_id,
                    evolving_session_items.len()
                );
            }

            for session_length in 1..evolving_session_items.len() {
                if cfg!(debug_assertions) {
                    eprintln!(
                        "  Updating session {} at session state {}",
                        evolving_session_id,
                        session_length,
                    );
                }
                let current_item = &evolving_session_items[session_length - 1];

                evolving_sessions_input.update(
                    (*evolving_session_id, *current_item),
                    session_length as isize,
                );

                let mut latency_in_micros: u128 = 0;

                time += 1;
                update_recommendations(
                    &mut recommmendations,
                    time,
                    &mut evolving_sessions_input,
                    worker,
                    &probe,
                    &mut trace,
                    &mut latency_in_micros,
                );

                latency_writer.write(format!("{},{}\n", session_length, latency_in_micros).as_ref()).unwrap();

                if !recommmendations.contains_key(evolving_session_id) {
                    eprintln!("No recommendations for session {}", evolving_session_id);
                    // std::process::exit(-1);
                }

                let srm: &HashMap<ItemId, f64> = recommmendations.get(evolving_session_id).unwrap();

                let session_recommendations: Vec<_> = srm
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|(item, score)| ScoredItem::new(*item, Similarity::new(*score, 1)))
                    .sorted()
                    .map(|scored| scored.itemid)
                    .filter(|item_id| *item_id != evolving_session_items[session_length - 1])
                    .take(20)
                    .collect();

                if session_recommendations.is_empty() {
                    eprintln!("WARNING: No recommendations for session {} after filtering.", evolving_session_id);
                    // std::process::exit(-1);
                }

                let recommended_items = session_recommendations.iter()
                    .map(|scored| scored.to_string())
                    .join(",");

                let observed_items = &evolving_session_items[session_length..].iter()
                    .map(|item| item.to_string())
                    .join(",");

                let line = format!("{};{}\n", recommended_items, observed_items);
                predictions_writer.write((line).as_ref()).unwrap();
            }

            recommmendations.clear()
        }
        predictions_writer.flush().unwrap();
        latency_writer.flush().unwrap();
    }).unwrap();
}
