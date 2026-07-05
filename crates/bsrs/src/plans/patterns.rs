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

/// Per-axis suffix products: `num_repeats[i] = prod(lengths[i+1..])`, the
/// number of consecutive rows an axis holds one value before advancing. Mirrors
/// `num_repeats` in bluesky's `snake_cyclers` (utils/__init__.py:688).
fn suffix_repeats(lengths: &[usize]) -> Vec<usize> {
    let mut num_repeats = vec![1usize; lengths.len()];
    for i in (0..lengths.len()).rev() {
        num_repeats[i] = if i + 1 < lengths.len() {
            num_repeats[i + 1] * lengths[i + 1]
        } else {
            1
        };
    }
    num_repeats
}

/// Map row `k` to axis `i`'s position index within `0..lengths[i]`, applying
/// snake (boustrophedon) folding when `snaked`. Without snake the index is the
/// plain mixed-radix digit `(k / num_repeats) % L`. With snake the period
/// doubles to `2L` and the second half counts back down (`2L-1-m`), so every
/// other pass of the axis runs in reverse — the exact effect of
/// `np.concatenate([v, v[::-1]])` in bluesky's `snake_cyclers`.
fn axis_index(k: usize, num_repeats: usize, l: usize, snaked: bool) -> usize {
    let snaked = snaked && l > 1;
    let period = if snaked { 2 * l } else { l };
    let m = (k / num_repeats) % period;
    if !snaked || m < l {
        m
    } else {
        2 * l - 1 - m
    }
}

/// `outer_product([(start1, stop1, num1), ...])` — N-D rectilinear grid.
/// Slowest axis varies first. Returns `prod(num_i)` rows, in natural
/// (non-snaked) order. The one-argument form of [`outer_product_snake`].
pub fn outer_product(axes: &[(f64, f64, usize)]) -> Vec<Vec<f64>> {
    outer_product_snake(axes, &[])
}

/// `outer_product_snake(axes, snaking)` — N-D rectilinear grid with per-axis
/// snake (boustrophedon) traversal. `snaking[i] == true` reverses axis `i` on
/// alternating passes to minimise dead travel; a missing / `false` entry keeps
/// the axis's natural left-to-right order. Ports bluesky's
/// `outer_product` + `snake_cyclers` (plan_patterns.py, utils/__init__.py:656).
/// Snaking the slowest axis (index 0) has no effect — it is traversed once.
pub fn outer_product_snake(axes: &[(f64, f64, usize)], snaking: &[bool]) -> Vec<Vec<f64>> {
    if axes.is_empty() || axes.iter().any(|(_, _, n)| *n == 0) {
        return Vec::new();
    }
    let lengths: Vec<usize> = axes.iter().map(|(_, _, n)| *n).collect();
    let total: usize = lengths.iter().product();
    let num_repeats = suffix_repeats(&lengths);
    let mut out = Vec::with_capacity(total);
    for k in 0..total {
        let mut row = Vec::with_capacity(axes.len());
        for i in 0..axes.len() {
            let (s, e, n) = axes[i];
            let snaked = snaking.get(i).copied().unwrap_or(false);
            let vi = axis_index(k, num_repeats[i], n, snaked);
            let t = if n > 1 {
                vi as f64 / (n as f64 - 1.0)
            } else {
                0.0
            };
            row.push(s + (e - s) * t);
        }
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

/// Outer-list product — N-D grid from per-axis lists, in natural (non-snaked)
/// order. The one-argument form of [`outer_list_product_snake`].
pub fn outer_list_product(axes: &[Vec<f64>]) -> Vec<Vec<f64>> {
    outer_list_product_snake(axes, &[])
}

/// `outer_list_product_snake(axes, snaking)` — N-D grid from per-axis position
/// lists with per-axis snake traversal, the list analog of
/// [`outer_product_snake`] (bluesky `outer_list_product` +`snake_cyclers`).
/// `snaking[i]` reverses axis `i` on alternating passes.
pub fn outer_list_product_snake(axes: &[Vec<f64>], snaking: &[bool]) -> Vec<Vec<f64>> {
    if axes.is_empty() || axes.iter().any(|v| v.is_empty()) {
        return Vec::new();
    }
    let lengths: Vec<usize> = axes.iter().map(|v| v.len()).collect();
    let total: usize = lengths.iter().product();
    let num_repeats = suffix_repeats(&lengths);
    let mut out = Vec::with_capacity(total);
    for k in 0..total {
        let mut row = Vec::with_capacity(axes.len());
        for i in 0..axes.len() {
            let snaked = snaking.get(i).copied().unwrap_or(false);
            let vi = axis_index(k, num_repeats[i], lengths[i], snaked);
            row.push(axes[i][vi]);
        }
        out.push(row);
    }
    out
}

/// `spiral(x_start, y_start, x_range, y_range, dr, nth, dr_y, tilt)` —
/// Archimedean spiral centred on `(x_start, y_start)`, a faithful port of
/// `bluesky.plan_patterns.spiral` (plan_patterns.py:18–77).
///
/// The spiral is walked by concentric **rings**: ring `i` sits at radius
/// `i·dr` and carries `i·nth` angular steps, so point density grows with
/// radius. Every candidate is clipped to the bounding box and the loop runs a
/// fixed ring count (`r_max/dr`-derived) rather than stopping at the first
/// point that pokes out, so the whole box — corners included — is filled.
///
/// - `dr` is the radial step along the minor (x) axis; `nth` the base angular
///   steps per ring.
/// - `dr_y` is the radial step along the major (y) axis; `None` ⇒ circular
///   (`dr_aspect = 1`), else `dr_aspect = dr_y/dr` stretches y and shrinks the
///   y half-extent to `y_range/(2·dr_aspect)`.
/// - `tilt` (radians) does **not** rotate the emitted coordinates; it shears
///   the *clip box* via `tilt_tan = tan(tilt + π/2)`, exactly as bluesky does.
///   At `tilt = 0`, `tilt_tan` is a huge finite number so the shear term
///   vanishes — matching numpy bit-for-bit rather than special-casing.
// Eight parameters mirror bluesky's `spiral(...)` positional API 1:1; bundling
// them into a struct would diverge from the port and the plan-level signature.
#[allow(clippy::too_many_arguments)]
pub fn spiral(
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    nth: usize,
    dr_y: Option<f64>,
    tilt: f64,
) -> Vec<(f64, f64)> {
    use std::f64::consts::PI;
    let mut out = Vec::new();
    if dr <= 0.0 || nth == 0 {
        return out;
    }
    let dr_aspect = match dr_y {
        None => 1.0,
        Some(dy) => dy / dr,
    };
    let half_x = x_range / 2.0;
    let half_y = y_range / (2.0 * dr_aspect);
    let r_max = (half_x * half_x + half_y * half_y).sqrt();
    // bluesky: num_ring = 1 + int(r_max/dr); rings iterate range(1, num_ring+2).
    let num_ring = 1 + (r_max / dr) as usize;
    let tilt_tan = (tilt + PI / 2.0).tan();
    for i_ring in 1..=(num_ring + 1) {
        let radius = i_ring as f64 * dr;
        let steps = i_ring * nth;
        let angle_step = 2.0 * PI / steps as f64;
        for i_angle in 0..steps {
            let angle = i_angle as f64 * angle_step;
            let x = radius * angle.cos();
            let y = radius * angle.sin() * dr_aspect;
            if (x - (y / dr_aspect) / tilt_tan).abs() <= half_x && (y / dr_aspect).abs() <= half_y {
                out.push((x_start + x, y_start + y));
            }
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

/// `spiral_fermat_pattern(x_start, y_start, x_range, y_range, dr, factor, dr_y,
/// tilt)` — Fermat (sunflower) spiral, a faithful port of
/// `bluesky.plan_patterns.spiral_fermat` (plan_patterns.py:200–257).
///
/// Point `i` sits at radius `√i · dr/factor` and golden angle `φ·i`, with `φ`
/// the **degree** constant `137.508°` (not the algebraic `π(3−√5)`) — kept
/// verbatim for point-set parity. The ring count is `int((1.5·diag·factor/dr)²)`.
///
/// - `dr` is the radial step along the minor (x) axis; larger `factor` divides
///   the radius (denser spiral).
/// - `dr_y`/`tilt` behave as in [`spiral`]: `dr_aspect = dr_y/dr` stretches y
///   and shrinks the y half-extent; `tilt` shears the clip box, it does not
///   rotate the coordinates.
///
/// One deliberate bluesky asymmetry is preserved: the y-clip here tests
/// `|y| ≤ half_y`, whereas [`spiral`] tests `|y/dr_aspect| ≤ half_y`. This is a
/// quirk of the upstream source, replicated for exact parity.
// Eight parameters mirror bluesky's `spiral_fermat(...)` positional API 1:1.
#[allow(clippy::too_many_arguments)]
pub fn spiral_fermat_pattern(
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    factor: f64,
    dr_y: Option<f64>,
    tilt: f64,
) -> Vec<(f64, f64)> {
    use std::f64::consts::PI;
    let mut out = Vec::new();
    if dr <= 0.0 || factor <= 0.0 {
        return out;
    }
    let dr_aspect = match dr_y {
        None => 1.0,
        Some(dy) => dy / dr,
    };
    let phi = 137.508 * PI / 180.0;
    let half_x = x_range / 2.0;
    let half_y = y_range / (2.0 * dr_aspect);
    let tilt_tan = (tilt + PI / 2.0).tan();
    let diag = (half_x * half_x + half_y * half_y).sqrt();
    // bluesky: num_rings = int((1.5*diag/(dr/factor))**2); range(1, num_rings).
    let base = 1.5 * diag / (dr / factor);
    let num_rings = (base * base) as usize;
    for i_ring in 1..num_rings {
        let radius = (i_ring as f64).sqrt() * dr / factor;
        let angle = phi * i_ring as f64;
        let x = radius * angle.cos();
        let y = radius * angle.sin() * dr_aspect;
        if (x - (y / dr_aspect) / tilt_tan).abs() <= half_x && y.abs() <= half_y {
            out.push((x_start + x, y_start + y));
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
    fn snake_reverses_fast_axis_on_odd_rows() {
        // 3x3 grid, values equal to indices. Fast axis (index 1) snaked:
        // row 0 forward, row 1 reversed, row 2 forward.
        let v = outer_product_snake(&[(0.0, 2.0, 3), (0.0, 2.0, 3)], &[false, true]);
        assert_eq!(
            v,
            vec![
                vec![0.0, 0.0],
                vec![0.0, 1.0],
                vec![0.0, 2.0],
                vec![1.0, 2.0],
                vec![1.0, 1.0],
                vec![1.0, 0.0],
                vec![2.0, 0.0],
                vec![2.0, 1.0],
                vec![2.0, 2.0],
            ]
        );
    }

    #[test]
    fn snake_empty_flags_equals_plain_product() {
        // Delegation invariant: outer_product == outer_product_snake(.., &[]).
        let axes = [(0.0, 1.0, 3), (10.0, 11.0, 2), (0.0, 4.0, 2)];
        assert_eq!(outer_product(&axes), outer_product_snake(&axes, &[]));
    }

    #[test]
    fn snake_on_slowest_axis_is_a_noop() {
        // The slowest axis is traversed once, so snaking it changes nothing.
        let axes = [(0.0, 1.0, 3), (10.0, 11.0, 2)];
        assert_eq!(
            outer_product_snake(&axes, &[true, false]),
            outer_product(&axes)
        );
    }

    #[test]
    fn snake_list_product_reverses_fast_axis() {
        let v = outer_list_product_snake(&[vec![1.0, 2.0], vec![10.0, 20.0, 30.0]], &[false, true]);
        assert_eq!(
            v,
            vec![
                vec![1.0, 10.0],
                vec![1.0, 20.0],
                vec![1.0, 30.0],
                vec![2.0, 30.0],
                vec![2.0, 20.0],
                vec![2.0, 10.0],
            ]
        );
    }

    #[test]
    fn snake_3d_is_a_continuous_walk() {
        // 2x2x2 cube, snake the two faster axes (bluesky snake_axes=True). The
        // resulting path must visit every cell exactly once and change exactly
        // one coordinate by one step between consecutive points (no fly-back).
        let v = outer_product_snake(
            &[(0.0, 1.0, 2), (0.0, 1.0, 2), (0.0, 1.0, 2)],
            &[false, true, true],
        );
        assert_eq!(
            v,
            vec![
                vec![0.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0],
                vec![0.0, 1.0, 1.0],
                vec![0.0, 1.0, 0.0],
                vec![1.0, 1.0, 0.0],
                vec![1.0, 1.0, 1.0],
                vec![1.0, 0.0, 1.0],
                vec![1.0, 0.0, 0.0],
            ]
        );
        // Independent check: adjacent points differ in exactly one axis by 1.0.
        for w in v.windows(2) {
            let diffs: usize = w[0]
                .iter()
                .zip(&w[1])
                .filter(|(a, b)| (*a - *b).abs() > 1e-9)
                .count();
            assert_eq!(diffs, 1, "snake walk must move one axis at a time");
        }
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

    // Ground truth for the parity tests: emitted by the verbatim bluesky
    // `spiral`/`spiral_fermat` bodies (plan_patterns.py) run under numpy. bsrs
    // must reproduce the same point set, in the same order, to parity.
    fn assert_points_eq(got: &[(f64, f64)], expected: &[(f64, f64)], case: &str) {
        assert_eq!(
            got.len(),
            expected.len(),
            "{case}: point count differs (got {}, want {})",
            got.len(),
            expected.len()
        );
        for (i, ((gx, gy), (ex, ey))) in got.iter().zip(expected).enumerate() {
            assert!(
                (gx - ex).abs() < 1e-9 && (gy - ey).abs() < 1e-9,
                "{case}: point {i} differs: got ({gx}, {gy}), want ({ex}, {ey})"
            );
        }
    }

    #[test]
    fn spiral_matches_bluesky_circular() {
        // The ±0.7071… points are cos/sin(45°) at radius 1 — i.e. 1/√2.
        let s = std::f64::consts::FRAC_1_SQRT_2;
        let pts = spiral(0.0, 0.0, 2.0, 2.0, 0.5, 4, None, 0.0);
        let expected = [
            (0.5, 0.0),
            (0.0, 0.5),
            (-0.5, 0.0),
            (0.0, -0.5),
            (1.0, 0.0),
            (s, s),
            (0.0, 1.0),
            (-s, s),
            (-1.0, 0.0),
            (-s, -s),
            (0.0, -1.0),
            (s, -s),
        ];
        assert_points_eq(&pts, &expected, "spiral circular");
    }

    #[test]
    fn spiral_matches_bluesky_elliptical_dr_y() {
        // dr_y=1.0 with dr=0.5 → dr_aspect=2: y is stretched, half_y shrinks.
        let pts = spiral(0.0, 0.0, 3.0, 2.0, 0.5, 4, Some(1.0), 0.0);
        let expected = [
            (0.5, 0.0),
            (0.0, 1.0),
            (-0.5, 0.0),
            (0.0, -1.0),
            (1.0, 0.0),
            (-1.0, 0.0),
            (1.5, 0.0),
            (-1.5, 0.0),
        ];
        assert_points_eq(&pts, &expected, "spiral elliptical");
    }

    #[test]
    fn spiral_tilt_shears_clip_box_not_ignored() {
        // tilt=1.3 clips the off-axis points that the untilted spiral keeps.
        let tilted = spiral(0.0, 0.0, 3.0, 1.0, 0.5, 4, None, 1.3);
        let expected = [
            (0.5, 0.0),
            (-0.5, 0.0),
            (1.0, 0.0),
            (-1.0, 0.0),
            (1.5, 0.0),
            (-1.5, 0.0),
        ];
        assert_points_eq(&tilted, &expected, "spiral tilted");
        // Same geometry untilted keeps two more (the ±0.5 y points), proving
        // tilt is actually applied rather than silently dropped.
        let untilted = spiral(0.0, 0.0, 3.0, 1.0, 0.5, 4, None, 0.0);
        assert_eq!(untilted.len(), 8, "untilted keeps the off-axis points");
    }

    #[test]
    fn spiral_fermat_matches_bluesky() {
        let pts = spiral_fermat_pattern(0.0, 0.0, 2.0, 2.0, 0.5, 1.0, None, 0.0);
        let expected = [
            (-0.368685829906, 0.337743628847),
            (0.061825124374, -0.704398789037),
            (0.526915019366, 0.687284920806),
            (-0.984710615984, -0.174198170965),
            (0.943359453551, -0.600060781419),
        ];
        assert_points_eq(&pts, &expected, "fermat");
    }

    #[test]
    fn spiral_fermat_matches_bluesky_dr_y_and_tilt() {
        let pts = spiral_fermat_pattern(0.0, 0.0, 4.0, 3.0, 0.3, 1.0, Some(0.6), 0.4);
        let expected = [
            (-0.221211497943, 0.405292354617),
            (-0.590826369591, -0.209037805158),
            (0.566015672130, -0.720072937702),
            (0.797030960067, 0.582208377454),
            (-0.831923727392, 0.686739868659),
            (1.056448806712, -0.464396032704),
            (-1.235878989709, 0.102041614983),
            (1.362479322045, 0.366879255344),
            (1.743333143432, -0.288371642246),
            (-1.841405851583, -0.341903435224),
        ];
        assert_points_eq(&pts, &expected, "fermat dr_y+tilt");
    }
}
