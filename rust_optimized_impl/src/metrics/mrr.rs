use std::cmp;
use itertools::Itertools;
use crate::metrics::SessionMetric;


pub struct Mrr {
    sum_of_scores: f64,
    qty: usize,
    length: usize,
}

impl Mrr {
}

impl Mrr {
    pub fn new(length: usize) -> Mrr {
        Mrr {
            sum_of_scores: 0_f64,
            qty: 0,
            length: length,
        }
    }
}

impl SessionMetric for Mrr {
    fn add(&mut self, recommendations: &Vec<u64>, next_items: &Vec<u64>) {
        self.qty += 1;
        let top_recos = recommendations.iter().take(cmp::min(recommendations.len(), self.length)).collect_vec();
        let next_item = next_items[0];
        let index = top_recos.iter().position(|&&item_id| item_id == next_item);
        match index {
            Some(rank) => {
                self.sum_of_scores += 1_f64 / (rank as f64 + 1_f64)
            },
            None => (),
        }
    }

    fn result(&self) -> f64 {
        self.sum_of_scores / self.qty as f64
    }

    fn get_name(&self) -> String {
        format!("Mrr@{}", self.length)
    }

}