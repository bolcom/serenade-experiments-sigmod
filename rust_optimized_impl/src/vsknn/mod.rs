use std::collections::BinaryHeap;
use std::cmp::Ordering;

use crate::vsknn::index_new::VSKnnIndexNew;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use crate::vsknn::offline_index::ProductAttributes;

pub mod index;
pub mod index_new;
pub mod hashed_index;
pub mod tree_index;
pub mod hashed_index_new;
pub mod hashed_index_var;
pub mod offline_index;

#[derive(PartialEq, Debug)]
pub struct SessionScore {
    pub id: u32,
    pub score: f64,
}

impl SessionScore {
    fn new(id: u32, score: f64) -> Self {
        SessionScore { id, score }
    }
}

/// Ordering for our max-heap, not that we must use a special implementation here as there is no
/// total order on floating point numbers.
fn cmp_reverse(scored_a: &SessionScore, scored_b: &SessionScore) -> Ordering {
    match scored_a.score.partial_cmp(&scored_b.score) {
        Some(Ordering::Less) => Ordering::Greater,
        Some(Ordering::Greater) => Ordering::Less,
        _ => Ordering::Equal,
    }
}

impl Eq for SessionScore {}

impl Ord for SessionScore {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_reverse(self, other)
    }
}

impl PartialOrd for SessionScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(cmp_reverse(self, other))
    }
}

#[derive(PartialEq, Debug)]
pub struct ItemScore {
    pub id: u64,
    pub score: f64,
}

impl ItemScore {
    fn new(id: u64, score: f64) -> Self {
        ItemScore { id, score }
    }
}

/// Ordering for our max-heap, not that we must use a special implementation here as there is no
/// total order on floating point numbers.
fn cmp_reverse2(scored_a: &ItemScore, scored_b: &ItemScore) -> Ordering {
    match scored_a.score.partial_cmp(&scored_b.score) {
        Some(Ordering::Less) => Ordering::Greater,
        Some(Ordering::Greater) => Ordering::Less,
        _ => Ordering::Equal,
    }
}

impl Eq for ItemScore {}

impl Ord for ItemScore {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_reverse2(self, other)
    }
}

impl PartialOrd for ItemScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(cmp_reverse2(self, other))
    }
}


fn linear_score(pos: usize) -> f64 {
    if pos < 100 { 1.0 - (0.1 * pos as f64) } else { 0.0 }
}

pub fn predict<I: VSKnnIndexNew + Send + Sync>(
    index: &I,
    evolving_session: &[u64],
    k: usize,
    m: usize,
    how_many: usize,
) -> BinaryHeap<ItemScore> {

    let neighbors = index.find_neighbors(evolving_session, k, m);

    let mut item_scores: HashMap<u64,f64> = HashMap::with_capacity(1000);

    for scored_session in neighbors.into_iter() {

        let training_item_ids:&Vec<u64> = index.items_for_session(&scored_session.id);

        let (first_match_index, _) = evolving_session.iter().rev().enumerate()
            .find(|(_, item_id)| training_item_ids.contains(*item_id))
            .unwrap();

        let first_match_pos = first_match_index + 1;

        let session_weight = linear_score(first_match_pos);

        for item_id in training_item_ids.iter() {
            let item_idf = index.idf(item_id);
            *item_scores.entry(*item_id).or_insert(0.0) +=
                session_weight * item_idf * scored_session.score;
        }
    }

    // Remove most recent item if it has been scored as well
    let most_recent_item = *evolving_session.last().unwrap();
    if let Entry::Occupied(entry) = item_scores.entry(most_recent_item.clone()) {
        entry.remove_entry();
    }

    fn passes_business_rules(current_item_attribs: Option<&ProductAttributes>, reco_item_attribs: Option<&ProductAttributes>) -> bool {
        if reco_item_attribs.is_none() {
            return false
        }
        let reco_attribs = reco_item_attribs.unwrap();
        if reco_attribs.is_for_sale == true {
            if reco_attribs.is_adult == true {
                if current_item_attribs.is_some() {
                    let current_attribs = current_item_attribs.unwrap();
                    return current_attribs.is_adult
                } else {
                    return false;
                }
            } else {
                return true
            }
        }
        return false
    }

    // Return the proper amount of recommendations and filter them using business rules.
    let mut top_items: BinaryHeap<ItemScore> = BinaryHeap::with_capacity(how_many);
    // let current_item_attribs: Option<&ProductAttributes> = index.find_attributes(&most_recent_item);
    for (reco_item_id, reco_item_score) in item_scores.into_iter() {
        let scored_item = ItemScore::new(reco_item_id, reco_item_score);

        if top_items.len() < how_many {
            // let reco_item_attribs:Option<&ProductAttributes> = index.find_attributes(&reco_item_id);
            // if passes_business_rules(current_item_attribs, reco_item_attribs) {
                top_items.push(scored_item);
            // }
        } else {
            let mut top = top_items.peek_mut().unwrap();
            if scored_item < *top {
                // let reco_item_attribs = index.find_attributes(&reco_item_id);
                // if passes_business_rules(current_item_attribs, reco_item_attribs) {
                    *top = scored_item;
                // }
            }
        }
    }

    top_items
}


