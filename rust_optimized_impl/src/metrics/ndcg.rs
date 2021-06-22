use std::cmp;
use itertools::Itertools;
use std::collections::HashSet;
use std::iter::FromIterator;
use crate::metrics::SessionMetric;


pub struct Ndcg {
    sum_of_scores: f64,
    qty: usize,
    length: usize,
}

impl Ndcg {
    fn dcg(&self, top_recos: &Vec<&u64>, next_items: &Vec<&u64>) -> f64 {
        let mut result = 0_f64;
        let next_items_set:HashSet<&u64> = HashSet::from_iter(next_items.iter().cloned());
        for (index , _item_id) in top_recos.iter().enumerate() {
            if next_items_set.contains(top_recos[index]) {
                if index == 0 {
                    result += 1_f64;
                } else {
                    result += 1 as f64 / ((index as f64) + 1_f64).log2();
                }
            }
        }
        result
    }


}

impl Ndcg {
    //
    /// Calculate Ndcg for predicted recommendations and the given next items that will be interacted with.
    ///
    ///
    /// ```
    /// mod metrics;
    /// use crate::metrics::ndcg::Ndcg;
    /// use crate::metrics::SessionMetric;
    /// let mut mymetric = Ndcg::new(20);
    /// let reco = vec![1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24];
    /// let next_items = vec![3,55,88,4];
    /// mymetric.add(reco, next_items);
    /// println!("{} {}", mymetric.get_name(), mymetric.result()); // Ndcg@20 0.36121211352040195
    /// ```
    pub fn new(length: usize) -> Ndcg {
        Ndcg {
            sum_of_scores: 0_f64,
            qty: 0,
            length: length,
        }
    }
}

impl SessionMetric for Ndcg {
    fn add(&mut self, recommendations: &Vec<u64>, next_items: &Vec<u64>) {
        let top_recos = recommendations.iter().take(cmp::min(recommendations.len(), self.length)).collect_vec();
        let top_next_items = next_items.iter().take(cmp::min(next_items.len(), self.length)).collect_vec();
        let next_items = next_items.iter().collect_vec();
        let dcg: f64 = self.dcg(&top_recos, &next_items);
        let dcg_max: f64 = self.dcg(&top_next_items, &next_items);
        self.sum_of_scores += dcg / dcg_max;
        self.qty += 1;
    }

    fn result(&self) -> f64 {
        self.sum_of_scores / self.qty as f64
    }

    fn get_name(&self) -> String {
        format!("Ndcg@{}", self.length)
    }

}