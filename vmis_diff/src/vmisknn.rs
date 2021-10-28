extern crate rand;

use std::collections::{HashMap, HashSet};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;

use itertools::Itertools;
use crate::differential::types::{SessionId, ItemId};
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

pub struct VMISkNN {
    pub historical_session_index: HashMap<SessionId, HashSet<ItemId>>,
    pub historical_session_max_order: HashMap<SessionId, u32>,
    pub historical_item_index: HashMap<ItemId, Vec<SessionId>>,
    pub item_idfs: HashMap<ItemId, f64>,
    pub k: usize,
    pub m: usize,
}

fn find_neighbors(
    k: usize,
    m: usize,
    evolving_session: &[ItemId],
    historical_item_index: &HashMap<ItemId, Vec<SessionId>>,
    historical_session_max_order: &HashMap<SessionId, u32>,
    historical_session_index: &HashMap<SessionId, HashSet<ItemId>>,
) -> BinaryHeap<SessionScore> {
    let num_items_in_evolving_session = evolving_session.len();

    let mut possible_neighbor_session_ids = HashSet::new();

    for session_item in evolving_session.iter() {
        possible_neighbor_session_ids.extend(historical_item_index[session_item].iter());
    }

    let sampled_neighbor_session_ids: HashSet<_> =
        // Avoid inspecting the order if we don't need to sample anyways
        if possible_neighbor_session_ids.len() <= m {
            possible_neighbor_session_ids
        } else {
            // TODO this could be faster with a heap
            possible_neighbor_session_ids.iter()
                .map(|session_id| (session_id, historical_session_max_order[session_id]))
                .sorted_by(|(_, order_a), (_, order_b)| Ordering::reverse(order_a.cmp(order_b)))
                .map(|(session_id, _order)| *session_id)
                .take(m)
                .collect()
        };

    let mut closest_neighbors: BinaryHeap<SessionScore> = BinaryHeap::with_capacity(k);

    for neighbor_session_id in sampled_neighbor_session_ids.into_iter() {
        let mut similarity = 0_f64;

        let other_session_items = &historical_session_index[&neighbor_session_id];

        // Decayed dot product
        for (pos, item_id) in evolving_session.iter().enumerate() {
            if other_session_items.contains(&item_id) {

                let decay_factor = (pos + 1) as f64 / num_items_in_evolving_session as f64;
                if cfg!(debug_assertions) {
                    println!(
                        "\t\tEncountered ;shared item {} at pos {} with session {} (decay {})",
                        item_id,
                        pos,
                        neighbor_session_id,
                        decay_factor
                    )
                }

                similarity += decay_factor;
            }
        }

        if similarity > 0.0 {
            // Update heap holding top-n scored items for this item
            let scored_session = SessionScore {
                id: neighbor_session_id,
                score: similarity,
            };

            if closest_neighbors.len() < k {
                closest_neighbors.push(scored_session);
            } else {
                let mut top = closest_neighbors.peek_mut().unwrap();
                if scored_session < *top {
                    *top = scored_session;
                }
            }
        }
    }

    closest_neighbors
}

fn idf(num_sessions_total: usize, num_session_with_item: usize) -> f64 {
    (num_sessions_total as f64 / num_session_with_item as f64).ln()
}

impl VMISkNN {

    pub fn new(
        k: usize,
        m: usize,
        interactions: Vec<(SessionId, ItemId, u32)>
    ) -> VMISkNN {

        let mut historical_session_index: HashMap<SessionId, HashSet<ItemId>> = HashMap::new();
        let mut historical_session_max_order: HashMap<SessionId, u32> = HashMap::new();
        let mut historical_item_index: HashMap<ItemId, Vec<SessionId>> = HashMap::new();

        for (session_id, item_id, order) in interactions.into_iter() {
            let session_items = historical_session_index.entry(session_id)
                .or_insert(HashSet::new());
            session_items.insert(item_id);

            let current_max_order = historical_session_max_order.entry(session_id).or_insert(order);
            if order > *current_max_order {
                *current_max_order = order;
            }

            let item_sessions = historical_item_index.entry(item_id).or_insert(Vec::new());
            item_sessions.push(session_id);
        }

        let num_historical_sessions = historical_session_index.len();

        let item_idfs: HashMap<ItemId, f64> = historical_item_index.iter()
            .map(|(item, session_ids)| {
                let item_idf = idf(num_historical_sessions, session_ids.len());

                (*item, item_idf)
            })
            .collect();

        VMISkNN {
            historical_session_index,
            historical_session_max_order,
            historical_item_index,
            item_idfs,
            k,
            m
        }
    }

    pub fn predict(&self, evolving_session: &[ItemId]) -> BinaryHeap<ItemScore> {
        let neighbors = self.find_neighbors(evolving_session);

        // if cfg!(debug_assertions) {
        //     for scored_session in &neighbors {
        //         eprintln!(
        //             "\t Found neighbor session {} with similarity {}",
        //             scored_session.id,
        //             scored_session.score
        //         );
        //     }
        // }

        let mut item_scores: HashMap<ItemId, f64> = HashMap::new();

        for scored_session in neighbors.into_iter() {

            let historical_session = &self.historical_session_index[&scored_session.id];

            let (first_match_index, _) = evolving_session.iter().rev().enumerate()
                .find(|(_, item_id)| historical_session.contains(*item_id))
                .unwrap();

            let first_match_pos = first_match_index + 1;

            let session_weight = crate::linear_score(first_match_pos);

            for item_id in historical_session.iter() {
                let item_idf = self.item_idfs[item_id];
                *item_scores.entry(*item_id).or_insert(0.0) +=
                    session_weight * item_idf * scored_session.score;
            }
        }

        // for (id, score) in &item_scores {
        //     eprintln!("    Item {} with score {}", id, score);
        // }

        // Remove most recent item if it has been scored as well
        if let Entry::Occupied(entry) = item_scores.entry(*evolving_session.last().unwrap()) {
            entry.remove_entry();
        }

        let mut top_items: BinaryHeap<ItemScore> = BinaryHeap::with_capacity(20);

        for (id, score) in item_scores.into_iter() {
            let scored_item = ItemScore { id, score };

            if top_items.len() < 20 {
                top_items.push(scored_item);
            } else {
                let mut top = top_items.peek_mut().unwrap();
                if scored_item < *top {
                    *top = scored_item;
                }
            }
        }

        top_items
    }

    fn find_neighbors(&self, evolving_session: &[ItemId]) -> BinaryHeap<SessionScore> {
        find_neighbors(
            self.k,
            self.m,
            evolving_session,
            &self.historical_item_index,
            &self.historical_session_max_order,
            &self.historical_session_index
        )
    }
}



#[cfg(test)]
mod tests {
    use crate::linear_score;
    use super::*;

    #[test]
    fn test_toy_example() {

        let historical_sessions = vec![
            (0, 1, 1), (0, 2, 1), (0, 3, 1), (0, 4, 1), (0, 5, 1), // [0 1 1 1 1 1]
            (1, 2, 1), // [0 0 0 0 0 1]
            (2, 3, 1), (2, 5, 1), // [0 0 0 1 0 1]
            (3, 2, 1), (3, 3, 1), (3, 5, 1), // [0 1 0 1 0 1]
        ];

        let k = 2;
        let m = 500;

        let vmis = VMISkNN::new(k, m, historical_sessions);

        // [0 1 3 2 0 0]
        let evolving_session = vec![2, 3, 1];

        /* Similarities:
            Session 0: sim([0 1 3 2 0 0], [0 1 1 1 1 1]) = 1 + 2/3 + 1/3 = 2
            Session 1: sim([0 1 3 2 0 0], [0 0 1 0 0 0]) = 1/3
            Session 2: sim([0 1 3 2 0 0], [0 0 0 1 0 1]) = 2/3
            Session 3: sim([0 1 3 2 0 0], [0 0 1 1 0 1]) = 1/3 + 2/3 = 1
         */

        let neighbors = find_neighbors(
            k,
            m,
            &evolving_session,
            &vmis.historical_item_index,
            &vmis.historical_session_max_order,
            &vmis.historical_session_index
        );

        assert_eq!(2, neighbors.len());
        assert!(neighbors.iter().find(|n| n.id == 0 && close_enough(2.0, n.score)).is_some());
        assert!(neighbors.iter().find(|n| n.id == 3 && close_enough(1.0, n.score)).is_some());


        let recos = vmis.predict(&evolving_session);


        let expected_4 = idf(4, 1) * (linear_score(1) * 2.0);
        let expected_5 = idf(4, 3) * (linear_score(1) * 2.0 + linear_score(2) * 1.0);

        println!("{}", expected_4);
        println!("{}", expected_5);

        println!("Found {} recommended items", recos.len());

        for scored_item in recos.iter() {
            println!("\tItem {} with score {}", scored_item.id, scored_item.score);
        }

        assert_eq!(4, recos.len());
        assert!(recos.iter().find(|i| i.id == 4 && close_enough(expected_4, i.score)).is_some());
        assert!(recos.iter().find(|i| i.id == 5 && close_enough(expected_5, i.score)).is_some());

        // [0 2 4 3 1 0]
        let evolving_session_two = vec![2, 3, 1, 4];

        let recos = vmis.predict(&evolving_session_two);

        for scored_item in recos.iter() {
            println!("\tItem {} with score {}", scored_item.id, scored_item.score);
        }

    }

    fn close_enough(expected: f64, actual: f64) -> bool {
        (expected - actual).abs() < 0.0001
    }
}