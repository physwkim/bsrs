//! Motion-efficient ordering of suggestions — bsrs's port of blop's
//! `route_suggestions`.
//!
//! blop solves an open-path TSP with networkx's simulated annealing (seeded,
//! greedy init). That is a heavy dependency for a handful of points and its
//! result depends on the annealer's RNG stream; this port uses
//! nearest-neighbour construction followed by 2-opt improvement, which is
//! deterministic and, for the point counts an optimizer suggests per step,
//! reaches the same or a shorter path. Only the visiting order can differ.

use super::protocol::{Params, Suggestion};

/// Reorder `suggestions` into a short open path over their numeric
/// dimensions, starting nearest `starting_position` when given. The routed
/// dimensions are the keys of the first suggestion whose value is a number;
/// a suggestion missing one of them contributes `NaN` distance and is
/// visited last. Fewer than two suggestions, or no numeric dimension, are
/// returned unchanged.
pub fn route_suggestions(
    suggestions: Vec<Suggestion>,
    starting_position: Option<&Params>,
) -> Vec<Suggestion> {
    if suggestions.len() < 2 {
        return suggestions;
    }
    let dims: Vec<&String> = suggestions[0]
        .params
        .iter()
        .filter(|(_, v)| v.is_number())
        .map(|(k, _)| k)
        .collect();
    if dims.is_empty() {
        return suggestions;
    }
    let coords = |p: &Params| -> Vec<f64> {
        dims.iter()
            .map(|d| p.get(*d).and_then(|v| v.as_f64()).unwrap_or(f64::NAN))
            .collect()
    };
    let points: Vec<Vec<f64>> = suggestions.iter().map(|s| coords(&s.params)).collect();
    let start = starting_position.map(coords);
    let order = route_index(&points, start.as_deref());
    let mut slots: Vec<Option<Suggestion>> = suggestions.into_iter().map(Some).collect();
    order
        .into_iter()
        .map(|i| slots[i].take().expect("route_index is a permutation"))
        .collect()
}

fn distance(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f64>()
        .sqrt()
}

/// Permutation of `0..points.len()` giving a short open path. With a start
/// point the path begins at the point nearest it; without, the path's first
/// point is free (blop's anchor node with zero-weight edges).
pub fn route_index(points: &[Vec<f64>], start: Option<&[f64]>) -> Vec<usize> {
    let n = points.len();
    if n < 2 {
        return (0..n).collect();
    }
    // Node `n` is the anchor: distance to every point is the start distance
    // (or 0 with no start); the path is a cycle from the anchor with the
    // closing edge free, i.e. an open path.
    let dist = |i: usize, j: usize| -> f64 {
        match (i == n, j == n) {
            (true, true) => 0.0,
            (true, false) => start.map_or(0.0, |s| distance(s, &points[j])),
            (false, true) => 0.0,
            (false, false) => distance(&points[i], &points[j]),
        }
    };
    // Nearest-neighbour construction from the anchor. A NaN distance never
    // compares smaller, so a point with a missing coordinate is taken last.
    let mut path = vec![n];
    let mut left: Vec<usize> = (0..n).collect();
    while !left.is_empty() {
        let here = *path.last().unwrap();
        let (k, _) = left
            .iter()
            .enumerate()
            .fold((0, f64::INFINITY), |(bk, bd), (k, &j)| {
                let d = dist(here, j);
                if d < bd {
                    (k, d)
                } else {
                    (bk, bd)
                }
            });
        path.push(left.swap_remove(k));
    }
    // 2-opt on the open path: reverse `path[i..=j]` when that shortens the
    // two edges it touches; the anchor stays fixed at index 0 and the last
    // edge (back to the anchor) is free, so a segment ending at the tail
    // only pays for its head edge.
    let edge = |a: usize, b: usize| dist(a, b);
    let mut improved = true;
    while improved {
        improved = false;
        for i in 1..path.len() - 1 {
            for j in i + 1..path.len() {
                let before = edge(path[i - 1], path[i])
                    + if j + 1 < path.len() {
                        edge(path[j], path[j + 1])
                    } else {
                        0.0
                    };
                let after = edge(path[i - 1], path[j])
                    + if j + 1 < path.len() {
                        edge(path[i], path[j + 1])
                    } else {
                        0.0
                    };
                if after + 1e-12 < before {
                    path[i..=j].reverse();
                    improved = true;
                }
            }
        }
    }
    path[1..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blop::protocol::TrialId;
    use serde_json::json;

    fn s(id: &str, x: f64) -> Suggestion {
        Suggestion::new(id, [("x1".to_string(), json!(x))])
    }

    fn ids(v: &[Suggestion]) -> Vec<String> {
        v.iter().map(|s| s.id.to_string()).collect()
    }

    #[test]
    fn single_suggestion_is_unchanged() {
        let out = route_suggestions(vec![s("only", 3.0)], None);
        assert_eq!(ids(&out), ["only"]);
    }

    #[test]
    fn starts_nearest_the_current_position() {
        let start: Params = [("x1".to_string(), json!(0.0))].into();
        let out = route_suggestions(vec![s("far", 10.0), s("near", 1.0)], Some(&start));
        assert_eq!(ids(&out), ["near", "far"]);
    }

    #[test]
    fn without_a_start_the_first_point_is_free() {
        let out = route_suggestions(vec![s("b", 5.0), s("c", 9.0), s("a", 1.0)], None);
        let order = ids(&out);
        assert!(
            order == ["a", "b", "c"] || order == ["c", "b", "a"],
            "{order:?}"
        );
    }

    #[test]
    fn two_opt_untangles_a_crossing() {
        // Points on a line visited greedily from 0: 1, 2, 10, 11, 5 is
        // nearest-neighbour-optimal only until 5 is reached late; 2-opt
        // must reorder to 1, 2, 5, 10, 11.
        let pts: Vec<Vec<f64>> = [1.0, 10.0, 2.0, 11.0, 5.0]
            .iter()
            .map(|x| vec![*x])
            .collect();
        let order = route_index(&pts, Some(&[0.0]));
        let xs: Vec<f64> = order.iter().map(|&i| pts[i][0]).collect();
        assert_eq!(xs, [1.0, 2.0, 5.0, 10.0, 11.0]);
    }

    #[test]
    fn non_numeric_dimensions_are_not_routed() {
        let mk = |id: &str, x: f64, c: &str| {
            Suggestion::new(
                id,
                [
                    ("x1".to_string(), json!(x)),
                    ("choice".to_string(), json!(c)),
                ],
            )
        };
        let start: Params = [("x1".to_string(), json!(0.0))].into();
        let out = route_suggestions(
            vec![mk("far", 10.0, "z"), mk("near", 1.0, "a")],
            Some(&start),
        );
        assert_eq!(ids(&out), ["near", "far"]);
        assert_eq!(out[0].id, TrialId::Str("near".into()));
    }

    #[test]
    fn missing_coordinate_is_visited_last() {
        let bare = Suggestion::new("bare", Vec::<(String, serde_json::Value)>::new());
        let start: Params = [("x1".to_string(), json!(0.0))].into();
        let out = route_suggestions(vec![s("far", 10.0), bare, s("near", 1.0)], Some(&start));
        assert_eq!(ids(&out), ["near", "far", "bare"]);
    }
}
