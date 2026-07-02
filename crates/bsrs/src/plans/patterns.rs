//! `bluesky.plan_patterns` equivalents — pure coordinate generators.
//!
//! These return concrete `Vec<Vec<f64>>` (one inner Vec per axis) or
//! `Vec<(f64, f64)>` for 2-D paths. Plans like `scan_nd`, `spiral`, and
//! `spiral_square` consume the output and emit the actual `Set`/`Wait`/`Read`
//! sequence.

#![allow(clippy::needless_range_loop)]

/// `inner_product(num, [(start1, stop1), (start2, stop2), ...])` —
/// linspaces all axes together. Each axis advances simultaneously.
/// Returns a vector of `num` rows, each row of length `axes.len()`.
pub fn inner_product(num: usize, axes: &[(f64, f64)]) -> Vec<Vec<f64>> {
    if num == 0 || axes.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(num);
    for i in 0..num {
        let t = if num > 1 {
            i as f64 / (num as f64 - 1.0)
        } else {
            0.0
        };
        let row = axes
            .iter()
            .map(|(s, e)| s + (e - s) * t)
            .collect::<Vec<_>>();
        out.push(row);
    }
    out
}

/// `outer_product([(start1, stop1, num1), ...])` — N-D rectilinear grid.
/// Slowest axis varies first. Returns `prod(num_i)` rows.
///
/// Per-axis `snake` in this Rust port is **not** implemented; the order
/// is always natural (no zig-zag). bluesky callers that rely on snake
/// must do their own reordering.
pub fn outer_product(axes: &[(f64, f64, usize)]) -> Vec<Vec<f64>> {
    if axes.is_empty() || axes.iter().any(|(_, _, n)| *n == 0) {
        return Vec::new();
    }
    let total: usize = axes.iter().map(|(_, _, n)| *n).product();
    let mut out = Vec::with_capacity(total);
    for k in 0..total {
        let mut row = Vec::with_capacity(axes.len());
        let mut idx = k;
        for (s, e, n) in axes.iter().rev() {
            let i = idx % n;
            idx /= n;
            let t = if *n > 1 {
                i as f64 / (*n as f64 - 1.0)
            } else {
                0.0
            };
            row.push(s + (e - s) * t);
        }
        row.reverse();
        out.push(row);
    }
    out
}

/// Inner-list product — like `inner_product` but the per-axis trajectories
/// are arbitrary lists (must all be the same length).
pub fn inner_list_product(axes: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n = axes.first().map(|v| v.len()).unwrap_or(0);
    if axes.iter().any(|v| v.len() != n) {
        return Vec::new();
    }
    (0..n)
        .map(|i| axes.iter().map(|v| v[i]).collect::<Vec<_>>())
        .collect()
}

/// Outer-list product — N-D grid from per-axis lists.
pub fn outer_list_product(axes: &[Vec<f64>]) -> Vec<Vec<f64>> {
    if axes.is_empty() || axes.iter().any(|v| v.is_empty()) {
        return Vec::new();
    }
    let mut out = vec![Vec::with_capacity(axes.len())];
    for axis in axes {
        let mut next = Vec::with_capacity(out.len() * axis.len());
        for prefix in &out {
            for v in axis {
                let mut row = prefix.clone();
                row.push(*v);
                next.push(row);
            }
        }
        out = next;
    }
    out
}

/// `spiral(x_start, y_start, x_range, y_range, dr, nth)` — Archimedean
/// spiral. `dr` is radial increment per turn; `nth` is angular subdivision
/// per turn. Returns `(x, y)` points until the spiral leaves the bounding
/// rectangle `[x_start - x_range/2, x_start + x_range/2]` × analog Y.
///
/// Mirrors `bluesky.plan_patterns.spiral` ignoring `dr_y` and `tilt`.
pub fn spiral(
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    nth: usize,
) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    if dr <= 0.0 || nth == 0 {
        return out;
    }
    let half_x = x_range / 2.0;
    let half_y = y_range / 2.0;
    let mut t = 0.0_f64;
    let dt = std::f64::consts::TAU / nth as f64;
    loop {
        let r = dr * t / std::f64::consts::TAU;
        let x = x_start + r * t.cos();
        let y = y_start + r * t.sin();
        if (x - x_start).abs() > half_x || (y - y_start).abs() > half_y {
            break;
        }
        out.push((x, y));
        t += dt;
        // hard cap so we don't go forever on degenerate inputs
        if out.len() > 1_000_000 {
            break;
        }
    }
    out
}

/// `spiral_square_pattern(x_center, y_center, x_range, y_range, x_num, y_num)` —
/// outward-traveling square spiral over a `x_num × y_num` rectilinear grid
/// centered on `(x_center, y_center)`. Returns the points in spiral order.
///
/// Mirrors bluesky's `spiral_square_pattern` for the *centered* layout.
pub fn spiral_square_pattern(
    x_center: f64,
    y_center: f64,
    x_range: f64,
    y_range: f64,
    x_num: usize,
    y_num: usize,
) -> Vec<(f64, f64)> {
    if x_num == 0 || y_num == 0 {
        return Vec::new();
    }
    let dx = if x_num > 1 {
        x_range / (x_num as f64 - 1.0)
    } else {
        0.0
    };
    let dy = if y_num > 1 {
        y_range / (y_num as f64 - 1.0)
    } else {
        0.0
    };
    let x0 = x_center - x_range / 2.0;
    let y0 = y_center - y_range / 2.0;

    // Generate the rectilinear grid in spiral order using a visited-mask walk.
    let total = x_num * y_num;
    let mut visited = vec![vec![false; x_num]; y_num];
    // Start at the center of the index grid; when even, pick the lower-right
    // of the four center cells (matches bluesky's pixel-centered convention).
    let mut ix = x_num / 2;
    let mut iy = y_num / 2;
    if x_num.is_multiple_of(2) {
        ix = ix.saturating_sub(1);
    }
    if y_num.is_multiple_of(2) {
        iy = iy.saturating_sub(1);
    }

    // Spiral move sequence: right, up, left, down, repeating with growing legs.
    let mut out = Vec::with_capacity(total);
    out.push((x0 + ix as f64 * dx, y0 + iy as f64 * dy));
    visited[iy][ix] = true;

    let dirs = [(1isize, 0isize), (0, 1), (-1, 0), (0, -1)];
    let mut leg = 1usize;
    let mut d = 0usize;
    while out.len() < total {
        for _ in 0..2 {
            let (dxi, dyi) = dirs[d];
            for _ in 0..leg {
                let nx = ix as isize + dxi;
                let ny = iy as isize + dyi;
                if nx < 0 || ny < 0 || nx >= x_num as isize || ny >= y_num as isize {
                    // step off-grid; just track index but don't emit
                    ix = (nx.max(0) as usize).min(x_num - 1);
                    iy = (ny.max(0) as usize).min(y_num - 1);
                    continue;
                }
                ix = nx as usize;
                iy = ny as usize;
                if !visited[iy][ix] {
                    visited[iy][ix] = true;
                    out.push((x0 + ix as f64 * dx, y0 + iy as f64 * dy));
                    if out.len() >= total {
                        return out;
                    }
                }
            }
            d = (d + 1) % 4;
        }
        leg += 1;
    }
    out
}

/// `spiral_fermat_pattern(x_start, y_start, x_range, y_range, dr, factor)` —
/// Fermat (sunflower) spiral with golden-angle increments. `dr` is the
/// radial step; `factor` typically `1.0`. Emits points whose coordinates
/// fall inside the bounding rect; stops when the radial distance
/// exceeds the rect diagonal.
pub fn spiral_fermat_pattern(
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    factor: f64,
) -> Vec<(f64, f64)> {
    use std::f64::consts::PI;
    if dr <= 0.0 || factor <= 0.0 {
        return Vec::new();
    }
    let golden = PI * (3.0 - 5.0_f64.sqrt());
    let half_x = x_range / 2.0;
    let half_y = y_range / 2.0;
    let max_r = (half_x * half_x + half_y * half_y).sqrt();
    let mut out = Vec::new();
    for n in 0..1_000_000 {
        let r = dr * factor * (n as f64).sqrt();
        if r > max_r {
            break;
        }
        let theta = golden * n as f64;
        let x = x_start + r * theta.cos();
        let y = y_start + r * theta.sin();
        if (x - x_start).abs() <= half_x && (y - y_start).abs() <= half_y {
            out.push((x, y));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inner_product_basic() {
        let v = inner_product(3, &[(0.0, 10.0), (5.0, 15.0)]);
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], vec![0.0, 5.0]);
        assert_eq!(v[1], vec![5.0, 10.0]);
        assert_eq!(v[2], vec![10.0, 15.0]);
    }

    #[test]
    fn outer_product_grid_size() {
        let v = outer_product(&[(0.0, 1.0, 3), (10.0, 11.0, 2)]);
        assert_eq!(v.len(), 6);
        // First three rows share x=0; last three share x=1; y alternates.
        assert_eq!(v[0], vec![0.0, 10.0]);
        assert_eq!(v[1], vec![0.0, 11.0]);
        assert_eq!(v[5], vec![1.0, 11.0]);
    }

    #[test]
    fn outer_list_product_size() {
        let v = outer_list_product(&[vec![1.0, 2.0], vec![10.0, 20.0, 30.0]]);
        assert_eq!(v.len(), 6);
    }

    #[test]
    fn spiral_square_visits_all_cells() {
        let pts = spiral_square_pattern(0.0, 0.0, 4.0, 4.0, 5, 5);
        assert_eq!(pts.len(), 25);
        // No duplicates.
        let mut keys: Vec<(i64, i64)> = pts
            .iter()
            .map(|(x, y)| ((x * 1e3) as i64, (y * 1e3) as i64))
            .collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 25);
    }

    #[test]
    fn spiral_archimedean_within_bounds() {
        let pts = spiral(0.0, 0.0, 10.0, 10.0, 0.5, 16);
        assert!(!pts.is_empty());
        assert!(pts
            .iter()
            .all(|(x, y)| x.abs() <= 5.0 + 1e-9 && y.abs() <= 5.0 + 1e-9));
    }
}
