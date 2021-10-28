use std::cmp::Ordering;
use std::rc::Rc;
use std::hash;

use std::mem;

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use std::hash::Hash;

pub type SessionId = u32;
pub type ItemId = u64;
pub type SessionItemWithOrder = (SessionId, ItemId, u32);
pub type OrderedSessionItem = (SessionId, (ItemId, Order));
pub type ItemScore = (ItemId, UnsafeF64);

pub type Trace<K, V, T, R> = TraceAgent<Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>>>;


#[derive(Eq, PartialEq, Debug, Clone, Abomonation, Hash)]
pub struct ScoredSession {
    pub session: SessionId,
    pub similarity: Similarity,
}

impl ScoredSession {
    pub fn new(session: SessionId, similarity: Similarity) -> Self {
        ScoredSession { session, similarity }
    }
}

impl Ord for ScoredSession {
    fn cmp(&self, other: &Self) -> Ordering {
        Ordering::reverse(self.similarity.cmp(&other.similarity))
            .then(self.session.cmp(&other.session))
    }
}

impl PartialOrd for ScoredSession {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Abomonation, Hash)]
pub struct ScoredItem {
    pub itemid: ItemId,
    pub similarity: Similarity,
}

impl ScoredItem {
    pub fn new(itemid: ItemId, similarity: Similarity) -> Self {
        ScoredItem { itemid, similarity }
    }
}

impl Ord for ScoredItem {
    fn cmp(&self, other: &Self) -> Ordering {
        Ordering::reverse(self.similarity.cmp(&other.similarity))
            .then(self.itemid.cmp(&other.itemid))
    }
}

impl PartialOrd for ScoredItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}


#[derive(Eq, PartialEq, Debug, Clone, Abomonation, Hash, Copy)]
pub struct Order {
    pub value: u32,
}

impl Order {
    pub fn new(value: u32) -> Self {
        Order { value }
    }
}

impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        Ordering::reverse(self.value.cmp(&other.value))
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}


#[derive(Debug, Clone, Abomonation)]
pub struct UnsafeF64 {
    pub value: f64,
}

impl UnsafeF64 {

    pub fn new(value: f64) -> Self {
        UnsafeF64 { value }
    }

    fn key(&self) -> u64 {
        unsafe { mem::transmute(self.value) }
    }

    pub fn add_assign(&mut self, other: &UnsafeF64) {
        self.value += other.value
    }

    pub fn weight_by(&self, other: &UnsafeF64) -> Self {
        UnsafeF64::new(self.value * other.value)
    }
}

impl hash::Hash for UnsafeF64 {
    fn hash<H>(&self, state: &mut H)
        where H: hash::Hasher
    {
        self.key().hash(state)
    }
}

impl PartialEq for UnsafeF64 {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for UnsafeF64 {}

impl Ord for UnsafeF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.partial_cmp(&other.value).unwrap()
    }
}

impl PartialOrd for UnsafeF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}


#[derive(Eq, PartialEq, Debug, Clone, Hash, Abomonation)]
pub struct Similarity {
    pub similarity: UnsafeF64,
    pub match_position: usize,
}

impl Similarity {

    pub fn new(similarity: f64, match_position: usize) -> Self {
        Similarity { similarity: UnsafeF64::new(similarity), match_position }
    }

    pub fn add_assign(&mut self, other: &Similarity) {
        self.similarity.add_assign(&other.similarity);
        if other.match_position < self.match_position {
            self.match_position = other.match_position;
        }
    }
}

impl Ord for Similarity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.similarity.partial_cmp(&other.similarity).unwrap()
    }
}

impl PartialOrd for Similarity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.similarity.partial_cmp(&other.similarity)
    }
}