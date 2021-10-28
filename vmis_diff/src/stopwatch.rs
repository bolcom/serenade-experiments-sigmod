use std::time::Instant;
use tdigest::TDigest;

#[derive(Clone)]
#[cfg_attr(feature = "use_serde", derive(Serialize, Deserialize))]
pub struct Stopwatch {
    start_time: Instant,
    prediction_durations:Vec<PositionDurationMicros>
}

pub type PositionDurationMicros = (u32, f64);

impl Stopwatch {
    pub fn new() -> Stopwatch {
        Stopwatch {
            start_time: Instant::now(),
            prediction_durations: Vec::new()
        }
    }

    pub fn start(&mut self) {
        self.start_time = Instant::now();
    }

    pub fn stop(&mut self, position_in_session:usize) {
        let duration = self.start_time.elapsed();
        let duration_as_micros:f64 = duration.as_micros() as f64;
        let tuple:PositionDurationMicros = (position_in_session as u32, duration_as_micros);
        self.prediction_durations.push(tuple);
    }

    pub fn get_n(&mut self) -> usize {
        self.prediction_durations.len()
    }

    pub fn get_percentile_in_micros(&mut self, q:f64) -> f64 {
        let t_digest = TDigest::new_with_size(100);
        let durations = self.prediction_durations.iter().map(|tuple| tuple.1).collect();
        let sorted_digest = t_digest.merge_unsorted(durations);
        let x = sorted_digest.estimate_quantile(q);
        x
    }

    pub fn get_raw_durations(&mut self) -> Vec<PositionDurationMicros> {
        let clone = self.prediction_durations.clone();
        clone
    }
}
