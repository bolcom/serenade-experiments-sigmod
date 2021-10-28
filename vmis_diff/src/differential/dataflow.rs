extern crate timely;
extern crate differential_dataflow;

use timely::order::TotalOrder;
use timely::dataflow::Scope;

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold, Reduce, Consolidate, Count};

use crate::differential::types::{SessionId, ItemId, ScoredSession, UnsafeF64, Similarity, OrderedSessionItem, Order, Trace};
use crate::linear_score;
use differential_dataflow::operators::arrange::Arranged;

pub (crate) fn prepare<G: Scope>(
    historical_sessions_with_duplicates: &Collection<G, OrderedSessionItem, isize>,
    m: usize,
    num_total_sessions: usize,
) -> (
        Collection<G, (SessionId, Order), isize>, // historical_session_max_order
        Collection<G, (ItemId, SessionId), isize>, // historical_sessions_by_item
        Collection<G, (ItemId, UnsafeF64), isize>, // item_idfs
    )
    where G::Timestamp: Lattice+Ord
{

    // Determine the latest interaction order per historical session
    let historical_session_max_order = historical_sessions_with_duplicates
        .map(|(session, (_item, order))| (session, order))
        .reduce(|_session, orders_and_multiplicities, output| {
            let (max_order, _) = orders_and_multiplicities[0];
            output.push((*max_order, 1));
        });

    let historical_sessions_by_item =
        historical_sessions_with_duplicates
            .join_map(
                &historical_session_max_order,
                |session, (item, _order), max_order| {
                    (*item, (*max_order, *session))
                }
            )
            // We only need to retain m historical sessions per item
            .reduce(move |_item, ordered_sessions_and_multiplicities, output| {
                for ((_order, historical_session), _) in ordered_sessions_and_multiplicities
                    .into_iter().take(m.clone())
                {
                    output.push((*historical_session, 1))
                }
            });

    // Count the number of sessions in which item occurs, and compute the weighted per-item idf
    let item_idfs = historical_sessions_with_duplicates
        .map(|(_session, (item, _order))| (item))
        //.count_total()
        .count()
        .map(move |(item, num_sessions)| {
            let item_idf = (num_total_sessions as f64 / num_sessions as f64).ln();

            (item, UnsafeF64::new(item_idf))
        });

    (historical_session_max_order, historical_sessions_by_item, item_idfs)
}


pub(crate) fn session_matches<G: Scope>(
    historical_sessions_by_item: &Collection<G, (ItemId, SessionId), isize>,
    evolving_sessions_by_item: &Collection<G, (ItemId, SessionId), isize>,
    historical_session_max_order: &Collection<G, (SessionId, Order), isize>,
    m: usize
) -> Collection<G, (SessionId, SessionId), isize>
    where G::Timestamp: Lattice+Ord {

    // Find all pairs of historical and evolving sessions that share at least one item
    let session_matches = evolving_sessions_by_item
        .join_map(&historical_sessions_by_item, |_, evolving_session, historical_session| {
            (*historical_session, *evolving_session)
        })
        .distinct();

    let sampled_session_matches = session_matches
        .join_map(
            &historical_session_max_order,
            |historical_session, evolving_session, order| {
                // Make sure we see matching sessions in descending order
                (*evolving_session, (*order, *historical_session))
            }
        )
        .reduce(move |_evolving_session, ordered_sessions_and_multiplicities, output| {
            for ((_order, historical_session), _) in ordered_sessions_and_multiplicities
                .into_iter().take(m.clone())
            {
                output.push((*historical_session, 1))
            }
        })
        .map(|(evolving_session, historical_session)| (historical_session, evolving_session));

    sampled_session_matches
}

pub(crate) fn similarities<G: Scope>(
    sampled_session_matches: &Collection<G, (SessionId, SessionId), isize>,
    historical_sessions_arranged_by_session: &Arranged<G, Trace<SessionId, ItemId, G::Timestamp, isize>>,
    evolving_sessions_by_session_and_item: &Collection<G, ((SessionId, ItemId), ()), isize>,
    evolving_session_lengths: &Collection<G, (SessionId, isize), isize>,
    k: usize
)   -> Collection<G, (SessionId, (SessionId, Similarity)), isize>
    where G::Timestamp: Lattice+Ord+TotalOrder {

    // Join historical session items [NOTE: We re-use the arrangement here]
    let session_matches_with_historical_items = historical_sessions_arranged_by_session
        .join_map(&sampled_session_matches, |historical_session, historical_item, evolving_session| {
            ((*evolving_session, *historical_item), *historical_session)
        });

    // Join historical session items
    let item_matches = evolving_sessions_by_session_and_item
        .join_map(
            &session_matches_with_historical_items,
            |(evolving_session, evolving_item), _, historical_session| {
                (*evolving_session, (*historical_session, *evolving_item))
            }
        )
        .join_map(
            &evolving_session_lengths,
            |evolving_session, (historical_session, historical_item), evolving_session_length| {
                ((*evolving_session, *historical_session),
                 (*historical_item, *evolving_session_length))
            }
        );

    // Compute pairwise similarities
    let similarities = item_matches
        .reduce(|_session_pair, items_lengths_and_multiplicities, output| {
            let mut similarity = 0.0;
            let mut minimal_match_position = std::usize::MAX;

            for ((_item, length), multiplicity) in items_lengths_and_multiplicities {
                let contribution = *multiplicity as f64 / *length as f64;
                similarity += contribution;

                // This might still give wrong results for duplicates, but should not matter much
                // in practice.
                let the_match = *length - *multiplicity + 1;
                let match_position = if the_match > 0 {
                    the_match as usize
                } else {
                    // The item occurred multiple times and we don't know its position.
                    // As a heuristic, we assume it was the most recent item, justified by the fact
                    // that it must be important if it has been visited multiple times.
                    1 as usize
                };

                if match_position < minimal_match_position {
                    minimal_match_position = match_position;
                }
            }
            output.push((Similarity::new(similarity, minimal_match_position), 1))
        })
        .consolidate()
        .map(|((evolving_session, historical_session), similarity)| {
            (evolving_session, ScoredSession::new(historical_session, similarity))
        })
        // Retain top-k similar historical sessions per evolving session
        .reduce(move |_, scored_sessions, output| {
            for (scored, _) in scored_sessions.iter().take(k.clone()) {
                output.push(((scored.session, scored.similarity.clone()), 1));
            }
        });

    similarities
}

pub(crate) fn item_scores<G: Scope>(
    similarities: &Collection<G, (SessionId, (SessionId, Similarity)), isize>,
    historical_sessions_arranged_by_session: &Arranged<G, Trace<SessionId, ItemId, G::Timestamp, isize>>,
    item_idfs: &Collection<G, (ItemId, UnsafeF64), isize>,
)   -> Collection<G, (SessionId, (ItemId, UnsafeF64)), isize>
    where G::Timestamp: Lattice+Ord+TotalOrder {

    let similarities_by_historical_session = similarities
        .map(|(evolving_session, (historical_session, similarity))| {
            (historical_session, (evolving_session, similarity))
        });

    // Form pairs of item matches [NOTE: We re-use the arrangement here]
    let item_scores = historical_sessions_arranged_by_session
        .join_map(
            &similarities_by_historical_session,
            |historical_session, historical_item, (evolving_session, similarity)| {
                ((*evolving_session, *historical_item),
                 (*historical_session, similarity.clone()))
            }
        )
        // Compute item scores by aggregating weighted similarities
        .reduce(|_session_and_item, similarities, output| {
            let mut item_score = UnsafeF64::new(0.0);
            for ((_, similarity), _) in similarities {
                let match_weight = UnsafeF64::new(linear_score(similarity.match_position));
                item_score.add_assign(&similarity.similarity.weight_by(&match_weight));
            }
            output.push((item_score, 1))
        });

    let weighted_item_scores = item_scores
        .map(|((evolving_session, item), similarity)| (item, (evolving_session, similarity)))
        .join_map(&item_idfs, |item, (evolving_session, similarity), item_idf| {
            (*evolving_session, (*item, similarity.weight_by(&item_idf)))
        });

    weighted_item_scores
}