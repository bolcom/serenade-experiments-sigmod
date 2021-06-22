extern crate hashbrown;

use hashbrown::HashSet;
use hashbrown::hash_map::DefaultHashBuilder as RandomState;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use crate::vsknn::SessionScore;

pub trait VSKnnIndex {

    fn items_for_session(&self, session: &u32) -> &HashSet<u64, RandomState>;

    fn idf(&self, item_id: &u64) -> f64;

    fn find_neighbors(
        &self,
        evolving_session: &[u64],
        k: usize,
        m: usize
    ) -> BinaryHeap<SessionScore>;
}


pub(crate) fn idf(num_sessions_total: usize, num_session_with_item: usize) -> f64 {
    (num_sessions_total as f64 / num_session_with_item as f64).ln()
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
