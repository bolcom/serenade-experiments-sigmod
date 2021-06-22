use crate::metrics::SessionMetric;

use std::cmp;
use itertools::Itertools;
use crate::io::{TrainingSessionId, ItemId, Time};
use itertools::__std_iter::FromIterator;
use std::collections::{HashMap, HashSet};


pub struct Popularity {
    sum_of_scores: f64,
    qty: usize,
    popularity_scores: HashMap<u64, i32>,
    length: usize,
    max_frequency: i32,
}

impl Popularity {}

impl Popularity {
    pub fn new(training_df: &Vec<(TrainingSessionId, ItemId, Time)>, length: usize) -> Popularity {
        let mut popularity_scores = HashMap::with_capacity(training_df.len());
        let mut max_frequency = 0;
        for (_session_id, item_id, _time) in training_df.iter() {
            let counter = popularity_scores.entry(item_id.clone()).or_insert(0);
            *counter += 1;
            max_frequency = cmp::max(counter.clone(), max_frequency);
        }

        Popularity {
            sum_of_scores: 0.0,
            qty: 0,
            popularity_scores: popularity_scores,
            length: length,
            max_frequency: max_frequency,
        }
    }
}

impl SessionMetric for Popularity {
    fn add(&mut self, recommendations: &Vec<u64>, _next_items: &Vec<u64>) {
        let items: HashSet<&u64> = HashSet::from_iter(recommendations.iter().take(cmp::min(recommendations.len(), self.length)).collect_vec().clone());
        self.qty += 1;
        if items.len() > 0 {
            let mut sum = 0_f64;
            for item in items.iter() {
                match self.popularity_scores.get(item) {
                    Some(item_freq) => { sum += item_freq.clone() as f64 / self.max_frequency as f64}
                    None => {}
                }
            }
            self.sum_of_scores += sum / items.len() as f64;
        }
    }

    fn result(&self) -> f64 {
        self.sum_of_scores / self.qty as f64
    }

    fn get_name(&self) -> String {
        format!("Popularity@{}", self.length)
    }

}