use serenade_optimized::{io, vsknn};
use serenade_optimized::vsknn::hashed_index_var::HashIndexVar;

use serenade_optimized::metrics::ndcg::Ndcg;
use serenade_optimized::metrics::SessionMetric;
use serenade_optimized::metrics::hitrate::HitRate;
use serenade_optimized::metrics::mrr::Mrr;
use core::cmp;
use serenade_optimized::metrics::coverage::Coverage;
use serenade_optimized::metrics::popularity::Popularity;
use serenade_optimized::vsknn::offline_index::OfflineIndex;
use std::fs::File;
use std::io::{LineWriter, Write};
use serenade_optimized::stopwatch::{Stopwatch, PositionDurationMicros};
use std::error::Error;


fn main() {
    // Train the model on a csv file and evaluate the predictions on the test dataset.
    // Its writes the prediction latencies to disk.
    let n_most_recent_sessions = 1000;
    let neighborhood_size_k = 500;
    let qty_max_reco_results = 21;

    let path_to_training = std::env::args().nth(1)
        .expect("Training data file not specified!");

    println!("training_data_file:{}", path_to_training);

    let test_data_file = std::env::args().nth(2)
        .expect("Test data file not specified!");
    println!("test_data_file:{}", test_data_file);

    let _predictions_output_file = std::env::args().nth(3)
        .expect("Predictions output file not specified!");

    let latencies_output_file = std::env::args().nth(4)
        .expect("Latencies output file not specified!");

    let vsknn_index = OfflineIndex::new_from_csv(&*path_to_training, n_most_recent_sessions);

    let ordered_test_sessions = io::read_test_data_evolving(&*test_data_file);

    let mut stopwatch = Stopwatch::new();
    ordered_test_sessions.iter().for_each(|(_session_id, evolving_session_items)| {
            for session_state in 1..evolving_session_items.len() {
                let session: &[u64] = &evolving_session_items[..session_state];
                stopwatch.start();
                let recommendations = vsknn::predict(&vsknn_index, &session, neighborhood_size_k, n_most_recent_sessions, qty_max_reco_results);
                stopwatch.stop(session_state);

                let _recommended_items = recommendations.into_sorted_vec().iter()
                    .map(|scored| scored.id).collect::<Vec<u64>>();

                let _next_items = Vec::from(&evolving_session_items[session_state..]);
            }
    });

    let latencies: Vec<PositionDurationMicros> = stopwatch.get_raw_durations();
    write_latencies(latencies, latencies_output_file).unwrap();
    fn write_latencies(latencies: Vec<PositionDurationMicros>, latencies_output_file: String) -> Result<(), Box<dyn Error>> {
        let file = File::create(latencies_output_file)?;
        let mut file = LineWriter::new(file);
        file.write(b"position,latency_in_micros\n")?;
        for (position, latency) in latencies.into_iter() {
            let line = format!("{},{}\n", position, latency);
            file.write(line.as_ref())?;
        }
        file.flush()?;
        Ok(())
    }
}
