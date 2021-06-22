pub mod ndcg;
pub mod mrr;
pub mod hitrate;
pub mod coverage;
pub mod popularity;


pub trait SessionMetric {
    fn add(&mut self, recommendations: &Vec<u64>, next_items: &Vec<u64>);
    fn result(&self) -> f64;
    fn get_name(&self) -> String;
}
