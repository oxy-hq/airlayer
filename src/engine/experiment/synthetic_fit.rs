//! The numerical core of the synthetic control: one treated series against a
//! donor pool, fitted on the pre window and projected over the post window.
//!
//! Every series is first demeaned by its OWN pre-window mean, so the fit
//! matches shape, not size: a big market is never "outside the hull" merely
//! for being big, and the estimate reads as a difference-in-differences with
//! weighted controls (Doudchenko & Imbens; Ferman & Pinto). Weights are
//! non-negative and sum to one, and the augmented correction of Ben-Michael,
//! Feller & Rothstein adds a ridge outcome model on what the weights leave
//! unmatched.
//!
//! The test refits every donor as a placebo against the rest of the pool, so a
//! `Pool` demeans each unit and forms the pre-window Gram matrix ONCE per
//! window; every fit then reads sub-matrices of it, and the ridge correction
//! is computed from the same Gram rather than from the series.

use crate::engine::experiment::PanelMatrix;

/// Every unit a core fits, demeaned over one window. Rows are local indices
/// into the `rows` the pool was built from.
pub(crate) struct Pool {
    /// Each unit's pre-window mean, added back to draw the series in levels.
    level: Vec<f64>,
    /// Demeaned pre-window and post-window slices.
    pre: Vec<Vec<f64>>,
    post: Vec<Vec<f64>>,
    /// `pre[a] · pre[b]` for every pair.
    gram: Vec<Vec<f64>>,
}

impl Pool {
    /// `pre` and `post` are retained-day index ranges `[p0, p1)`, `[q0, q1)`.
    fn new(m: &PanelMatrix, rows: &[usize], pre: (usize, usize), post: (usize, usize)) -> Pool {
        let mut level = Vec::with_capacity(rows.len());
        let (mut pre_rows, mut post_rows) = (Vec::new(), Vec::new());
        for &u in rows {
            let mean = (pre.0..pre.1).map(|d| m.get(u, d)).sum::<f64>() / (pre.1 - pre.0) as f64;
            level.push(mean);
            pre_rows.push(
                (pre.0..pre.1)
                    .map(|d| m.get(u, d) - mean)
                    .collect::<Vec<_>>(),
            );
            post_rows.push(
                (post.0..post.1)
                    .map(|d| m.get(u, d) - mean)
                    .collect::<Vec<_>>(),
            );
        }
        let gram = pre_rows
            .iter()
            .map(|a| pre_rows.iter().map(|b| dot(a, b)).collect())
            .collect();
        Pool {
            level,
            pre: pre_rows,
            post: post_rows,
            gram,
        }
    }
}

/// The two pools every fit reads: the registered windows, and the holdout
/// split of the pre window (fit on its start, predict its last days).
pub(crate) struct Pools {
    main: Pool,
    holdout: Pool,
}

/// Days held out at the end of the pre window: as long as the post window,
/// but never more than a third of the pre window.
fn holdout_days(pre: (usize, usize), post: (usize, usize)) -> usize {
    (post.1 - post.0).min((pre.1 - pre.0) / 3).max(1)
}

impl Pools {
    pub(crate) fn new(
        m: &PanelMatrix,
        rows: &[usize],
        pre: (usize, usize),
        post: (usize, usize),
    ) -> Pools {
        let h = holdout_days(pre, post);
        Pools {
            main: Pool::new(m, rows, pre, post),
            holdout: Pool::new(m, rows, (pre.0, pre.1 - h), (pre.1 - h, pre.1)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Fit {
    /// Donor weights, aligned with the `donors` the fit was given.
    pub(crate) weights: Vec<f64>,
    /// Mean of `actual − synthetic` over the post window.
    pub(crate) effect: f64,
    /// RMS of the convex fit's in-sample pre-window residual, for display and
    /// Abadie's post/pre ratio. Neither the refusal threshold nor the test
    /// reads it: both use `holdout_rmspe`.
    pub(crate) pre_rmspe: f64,
    /// RMS of the unit's own demeaned pre series: its volatility around its
    /// own mean, i.e. what a flat line would score.
    pub(crate) pre_sd: f64,
    pub(crate) post_rmspe: f64,
    /// RMS of the gap over the last days of the pre window, predicted — with
    /// the augmented correction, exactly as the post window is — by a fit that
    /// never saw them: what a post-window gap of pure noise looks like for
    /// THIS unit. In-sample error shrinks with overfitting, more for a unit the
    /// pool tracks closely, so scaling a test by it rejects too often; the
    /// holdout error does not shrink that way.
    pub(crate) holdout_rmspe: f64,
    /// The unit's pre-window mean, added back to draw the series in levels.
    pub(crate) level: f64,
    /// Synthetic values in levels over the pre window, then the post window.
    pub(crate) synthetic_pre: Vec<f64>,
    pub(crate) synthetic_post: Vec<f64>,
}

impl Fit {
    /// Holdout error over the unit's own volatility: what the registered
    /// `max_pre_fit` gates on. 0 is a perfect track, 1 a flat line; NaN when
    /// the unit is flat over the pre window and the ratio means nothing.
    pub(crate) fn pre_fit(&self) -> f64 {
        if self.pre_sd > 0.0 {
            self.holdout_rmspe / self.pre_sd
        } else {
            f64::NAN
        }
    }
}

const MAX_ITER: usize = 5_000;
const TOL: f64 = 1e-10;

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn rms(v: &[f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    (v.iter().map(|x| x * x).sum::<f64>() / v.len() as f64).sqrt()
}

/// Euclidean projection onto the probability simplex (Duchi et al. 2008).
fn project_simplex(v: &[f64]) -> Vec<f64> {
    let mut u = v.to_vec();
    u.sort_by(|a, b| b.total_cmp(a));
    let (mut cum, mut theta) = (0.0, 0.0);
    for (i, x) in u.iter().enumerate() {
        cum += x;
        let t = (cum - 1.0) / (i + 1) as f64;
        if x - t > 0.0 {
            theta = t;
        }
    }
    v.iter().map(|x| (x - theta).max(0.0)).collect()
}

/// An upper bound on the largest eigenvalue of a symmetric PSD matrix, for the
/// gradient step. Power iteration converges from below, so its estimate gets
/// headroom, and it is capped by two bounds that always hold — the trace and
/// the largest absolute row sum — which also stand in when the start vector
/// sits in the null space.
fn lipschitz(g: &[Vec<f64>]) -> f64 {
    let trace: f64 = (0..g.len()).map(|i| g[i][i]).sum();
    let rows = g
        .iter()
        .map(|r| r.iter().map(|x| x.abs()).sum::<f64>())
        .fold(0.0, f64::max);
    let ceiling = trace.min(rows);
    let n = g.len();
    let mut v = vec![1.0 / (n as f64).sqrt(); n];
    let mut lambda = 0.0;
    for _ in 0..200 {
        let w: Vec<f64> = g.iter().map(|row| dot(row, &v)).collect();
        let norm = dot(&w, &w).sqrt();
        if norm == 0.0 {
            return ceiling;
        }
        lambda = norm;
        v = w.into_iter().map(|x| x / norm).collect();
    }
    (lambda * 1.1).min(ceiling)
}

/// `min ‖x − Σ w_j d_j‖²` over the simplex, by accelerated projected gradient
/// on the donors' Gram matrix `gram` and their products with the target `b`
/// (`O(J²)` per step, independent of window length).
pub(crate) fn simplex_weights(gram: &[Vec<f64>], b: &[f64]) -> Vec<f64> {
    let j = gram.len();
    let step_size = lipschitz(gram);
    let mut w = vec![1.0 / j as f64; j];
    if step_size <= 0.0 {
        return w;
    }
    let (mut y, mut t) = (w.clone(), 1.0_f64);
    for _ in 0..MAX_ITER {
        let grad: Vec<f64> = (0..j).map(|r| dot(&gram[r], &y) - b[r]).collect();
        let step: Vec<f64> = (0..j).map(|r| y[r] - grad[r] / step_size).collect();
        let next = project_simplex(&step);
        let t_next = (1.0 + (1.0 + 4.0 * t * t).sqrt()) / 2.0;
        let moved: f64 = next.iter().zip(&w).map(|(a, c)| (a - c).abs()).sum();
        y = (0..j)
            .map(|r| next[r] + (t - 1.0) / t_next * (next[r] - w[r]))
            .collect();
        w = next;
        t = t_next;
        if moved < TOL {
            break;
        }
    }
    w
}

/// Solve `A x = b` for a symmetric positive-definite `A` (Cholesky).
fn solve_spd(a: &[Vec<f64>], b: &[f64]) -> Option<Vec<f64>> {
    let n = a.len();
    let mut l = vec![vec![0.0; n]; n];
    for i in 0..n {
        for k in 0..=i {
            let s: f64 = (0..k).map(|p| l[i][p] * l[k][p]).sum();
            if i == k {
                let d = a[i][i] - s;
                if d <= 0.0 {
                    return None;
                }
                l[i][i] = d.sqrt();
            } else {
                l[i][k] = (a[i][k] - s) / l[k][k];
            }
        }
    }
    let mut y = vec![0.0; n];
    for i in 0..n {
        y[i] = (b[i] - (0..i).map(|p| l[i][p] * y[p]).sum::<f64>()) / l[i][i];
    }
    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        x[i] = (y[i] - (i + 1..n).map(|p| l[p][i] * x[p]).sum::<f64>()) / l[i][i];
    }
    Some(x)
}

/// The augmented correction per post day: `rᵀ η_t`, where `r` is the convex
/// fit's pre residual and `η_t` the ridge regression of the donors' post value
/// on their pre series, solved in its dual (`J × J`) form. The penalty is the
/// donors' mean squared centred pre norm — a fixed rule, so a refit is
/// reproducible. Everything pre-window comes from the Gram: with `G` the
/// donors' Gram, `b` their products with the target and `w` the weights,
/// `xᵢ·r = bᵢ − (G w)ᵢ`, and centring is `H G H`.
fn ridge_correction(g: &[Vec<f64>], b: &[f64], w: &[f64], post: &[&[f64]]) -> Vec<f64> {
    let (j, t1) = (g.len(), post[0].len());
    let jf = j as f64;
    let row_mean: Vec<f64> = g.iter().map(|r| r.iter().sum::<f64>() / jf).collect();
    let all_mean = row_mean.iter().sum::<f64>() / jf;
    let mut k: Vec<Vec<f64>> = (0..j)
        .map(|a| {
            (0..j)
                .map(|c| g[a][c] - row_mean[a] - row_mean[c] + all_mean)
                .collect()
        })
        .collect();
    let lambda = (0..j).map(|i| k[i][i]).sum::<f64>() / jf;
    if lambda.is_nan() || lambda <= 0.0 {
        return vec![0.0; t1];
    }
    for (i, row) in k.iter_mut().enumerate() {
        row[i] += lambda;
    }
    let xr: Vec<f64> = (0..j).map(|i| b[i] - dot(&g[i], w)).collect();
    let xr_mean = xr.iter().sum::<f64>() / jf;
    let rx: Vec<f64> = xr.iter().map(|v| v - xr_mean).collect();
    // rᵀ Xcᵀ (XcXcᵀ + λI)⁻¹ Yc: one J-vector solve serves every post day.
    let Some(alpha) = solve_spd(&k, &rx) else {
        return vec![0.0; t1];
    };
    let alpha_sum: f64 = alpha.iter().sum();
    (0..t1)
        .map(|t| {
            let col_mean = post.iter().map(|r| r[t]).sum::<f64>() / jf;
            (0..j).map(|i| alpha[i] * post[i][t]).sum::<f64>() - alpha_sum * col_mean
        })
        .collect()
}

/// Fit pool row `unit` against pool rows `donors`.
pub(crate) fn fit(pools: &Pools, unit: usize, donors: &[usize]) -> Fit {
    let holdout = project(&pools.holdout, unit, donors).gaps;
    let mut f = project(&pools.main, unit, donors).fit;
    f.holdout_rmspe = rms(&holdout);
    f
}

struct Projected {
    fit: Fit,
    gaps: Vec<f64>,
}

fn project(p: &Pool, unit: usize, donors: &[usize]) -> Projected {
    let g: Vec<Vec<f64>> = donors
        .iter()
        .map(|a| donors.iter().map(|c| p.gram[*a][*c]).collect())
        .collect();
    let b: Vec<f64> = donors.iter().map(|d| p.gram[*d][unit]).collect();
    let weights = simplex_weights(&g, &b);
    let combine = |rows: &[Vec<f64>]| -> Vec<f64> {
        let len = rows[unit].len();
        (0..len)
            .map(|t| {
                weights
                    .iter()
                    .zip(donors)
                    .map(|(w, d)| w * rows[*d][t])
                    .sum()
            })
            .collect()
    };
    let target = &p.pre[unit];
    let synth_pre = combine(&p.pre);
    let residual: Vec<f64> = target.iter().zip(&synth_pre).map(|(a, s)| a - s).collect();
    let donor_post: Vec<&[f64]> = donors.iter().map(|d| p.post[*d].as_slice()).collect();
    let correction = ridge_correction(&g, &b, &weights, &donor_post);
    let synth_post: Vec<f64> = combine(&p.post)
        .into_iter()
        .zip(&correction)
        .map(|(s, c)| s + c)
        .collect();
    let gaps: Vec<f64> = p.post[unit]
        .iter()
        .zip(&synth_post)
        .map(|(a, s)| a - s)
        .collect();
    let level = p.level[unit];
    let fit = Fit {
        effect: gaps.iter().sum::<f64>() / gaps.len() as f64,
        pre_rmspe: rms(&residual),
        pre_sd: rms(target),
        post_rmspe: rms(&gaps),
        holdout_rmspe: f64::NAN,
        level,
        synthetic_pre: synth_pre.iter().map(|s| s + level).collect(),
        synthetic_post: synth_post.iter().map(|s| s + level).collect(),
        weights,
    };
    Projected { fit, gaps }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn experiment_synthetic_simplex_projection_lands_on_the_simplex() {
        let p = project_simplex(&[0.9, 0.8, -3.0, 0.1]);
        assert!((p.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert!(p.iter().all(|x| *x >= 0.0));
        assert!(
            (p[0] - 0.55).abs() < 1e-12 && (p[1] - 0.45).abs() < 1e-12,
            "{p:?}"
        );
        assert_eq!(project_simplex(&[0.2, 0.3, 0.5]), vec![0.2, 0.3, 0.5]);
    }

    #[test]
    fn experiment_synthetic_weights_recover_an_exact_convex_mix() {
        // target = 0.7·a + 0.3·b, with c a decoy: the weights must find it.
        let a: Vec<f64> = (0..40).map(|t| (t as f64 * 0.37).sin()).collect();
        let b: Vec<f64> = (0..40).map(|t| (t as f64 * 0.91).cos()).collect();
        let c: Vec<f64> = (0..40).map(|t| ((t * 7 % 11) as f64) - 5.0).collect();
        let target: Vec<f64> = a.iter().zip(&b).map(|(x, y)| 0.7 * x + 0.3 * y).collect();
        let rows = [a, b, c];
        let gram: Vec<Vec<f64>> = rows
            .iter()
            .map(|x| rows.iter().map(|y| dot(x, y)).collect())
            .collect();
        let bt: Vec<f64> = rows.iter().map(|x| dot(x, &target)).collect();
        let w = simplex_weights(&gram, &bt);
        assert!(
            (w[0] - 0.7).abs() < 1e-6 && (w[1] - 0.3).abs() < 1e-6,
            "{w:?}"
        );
        assert!(w[2].abs() < 1e-6, "{w:?}");
    }

    #[test]
    fn experiment_synthetic_cholesky_solves_a_known_system() {
        let a = vec![vec![4.0, 2.0], vec![2.0, 3.0]];
        let x = solve_spd(&a, &[2.0, 1.0]).expect("SPD");
        assert!((x[0] - 0.5).abs() < 1e-12 && x[1].abs() < 1e-12, "{x:?}");
        assert!(solve_spd(&[vec![0.0]], &[1.0]).is_none());
    }

    #[test]
    fn experiment_synthetic_step_bound_never_undershoots_the_top_eigenvalue() {
        // Two nearly equal top eigenvalues (power iteration stalls between
        // them) and a start vector in the null space: both must still give a
        // bound at or above λmax = 2.
        let close = vec![
            vec![2.0, 0.0, 0.0],
            vec![0.0, 1.999, 0.0],
            vec![0.0, 0.0, 0.1],
        ];
        assert!(lipschitz(&close) >= 2.0);
        let null_start = vec![vec![1.0, -1.0], vec![-1.0, 1.0]];
        assert!((lipschitz(&null_start) - 2.0).abs() < 1e-12);
    }
}
