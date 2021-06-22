use crate::metrics::SessionMetric;

use std::cmp;
use itertools::Itertools;
use hashbrown::HashSet;
use crate::io::{TrainingSessionId, ItemId, Time};


pub struct Coverage {
    unique_training_items: usize,
    test_items: HashSet<u64>,
    length: usize,
}

impl Coverage {}

impl Coverage {
    pub fn new(training_df: &Vec<(TrainingSessionId, ItemId, Time)>, length: usize) -> Coverage {
        let mut distinct_item_ids = training_df.iter().map(|record| record.1).collect_vec();
        distinct_item_ids.sort();
        distinct_item_ids.dedup();
        Coverage {
            unique_training_items: distinct_item_ids.len(),
            test_items: HashSet::new(),
            length: length,
        }
    }
}

impl SessionMetric for Coverage {
    fn add(&mut self, recommendations: &Vec<u64>, _next_items: &Vec<u64>) {
        let top_recos = recommendations.iter().take(cmp::min(recommendations.len(), self.length)).collect_vec();
        for item_id in top_recos.into_iter() {
            self.test_items.insert(item_id.clone());
        }
    }

    fn result(&self) -> f64 {
        self.test_items.len() as f64 / self.unique_training_items as f64
    }

    fn get_name(&self) -> String {
        format!("Coverage@{}", self.length)
    }

}