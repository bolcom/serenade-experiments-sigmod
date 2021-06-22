extern crate hashbrown;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use crate::vsknn::SessionScore;
use crate::vsknn::offline_index::ProductAttributes;

pub trait VSKnnIndexNew {

    fn items_for_session(&self, session: &u32) -> &Vec<u64>;

    fn idf(&self, item_id: &u64) -> f64;

    fn find_neighbors(
        &self,
        evolving_session: &[u64],
        k: usize,
        m: usize
    ) -> BinaryHeap<SessionScore>;

    fn find_attributes(&self, item_id: &u64) -> Option<&ProductAttributes>;
}

#[derive(Eq, Debug)]
pub struct SessionTime {
    pub session_id: u32,
    pub time: u32,
}

impl SessionTime {
    pub fn new(session_id: u32, time: u32) -> Self {
        SessionTime { session_id, time }
    }
}

impl Ord for SessionTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for SessionTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SessionTime {
    fn eq(&self, other: &Self) -> bool {
        // == is defined as being based on the contents of an object.
        self.session_id == other.session_id
    }
}
