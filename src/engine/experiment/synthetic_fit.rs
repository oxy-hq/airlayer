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

/// A treated (or placebo) unit's fit: rows are panel unit indices, windows are
/// retained-day index ranges `[p0, p1)` and `[q0, q1)`.
pub(crate) struct FitInput<'a> {
    pub(crate) m: &'a crate::engine::experiment::PanelMatrix,
    pub(crate) unit: usize,
    pub(crate) donors: &'a [usize],
    pub(crate) pre: (usize, usize),
    pub(crate) post: (usize, usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Fit {
    /// Donor weights, aligned with `FitInput::donors`.
    pub(crate) weights: Vec<f64>,
    /// Mean of `actual − synthetic` over the post window.
    pub(crate) effect: f64,
    /// RMS of the convex fit's pre-window residual. The augmented correction
    /// is NOT credited here: the pre fit is judged on what the weights alone
    /// achieve, which is the conservative reading.
    pub(crate) pre_rmspe: f64,
    /// RMS of the unit's own demeaned pre series: its volatility around its
    /// own mean, i.e. what a flat line would score.
    pub(crate) pre_sd: f64,
    pub(crate) post_rmspe: f64,
    /// RMS of the gap over the last days of the pre window, predicted by a fit
    /// that never saw them: what a post-window gap of pure noise looks like for
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

const MAX_ITER: usize = 5_000;
const TOL: f64 = 1e-10;

fn demeaned(
    m: &crate::engine::experiment::PanelMatrix,
    u: usize,
    pre: (usize, usize),
) -> (f64, Vec<f64>) {
    let level = (pre.0..pre.1).map(|d| m.get(u, d)).sum::<f64>() / (pre.1 - pre.0) as f64;
    let row = (0..m.n_days()).map(|d| m.get(u, d) - level).collect();
    (level, row)
}

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

/// Largest eigenvalue of a symmetric PSD matrix by power iteration.
fn top_eigen(g: &[Vec<f64>]) -> f64 {
    let n = g.len();
    let mut v = vec![1.0 / (n as f64).sqrt(); n];
    let mut lambda = 0.0;
    for _ in 0..200 {
        let w: Vec<f64> = g.iter().map(|row| dot(row, &v)).collect();
        let norm = dot(&w, &w).sqrt();
        if norm == 0.0 {
            return 0.0;
        }
        lambda = norm;
        v = w.into_iter().map(|x| x / norm).collect();
    }
    lambda
}

/// `min ‖x − Σ w_j d_j‖²` over the simplex, by accelerated projected gradient
/// on the donors' Gram matrix (`O(J²)` per step, independent of window length).
pub(crate) fn simplex_weights(target: &[f64], donors: &[Vec<f64>]) -> Vec<f64> {
    let j = donors.len();
    let gram: Vec<Vec<f64>> = donors
        .iter()
        .map(|a| donors.iter().map(|b| dot(a, b)).collect())
        .collect();
    let b: Vec<f64> = donors.iter().map(|d| dot(d, target)).collect();
    let lipschitz = top_eigen(&gram);
    let mut w = vec![1.0 / j as f64; j];
    if lipschitz <= 0.0 {
        return w;
    }
    let (mut y, mut t) = (w.clone(), 1.0_f64);
    for _ in 0..MAX_ITER {
        let grad: Vec<f64> = (0..j).map(|r| dot(&gram[r], &y) - b[r]).collect();
        let step: Vec<f64> = (0..j).map(|r| y[r] - grad[r] / lipschitz).collect();
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
/// donors' mean squared pre norm — a fixed rule, so a refit is reproducible.
fn ridge_correction(residual: &[f64], pre: &[Vec<f64>], post: &[Vec<f64>]) -> Vec<f64> {
    let (j, t0, t1) = (pre.len(), residual.len(), post[0].len());
    let centre = |rows: &[Vec<f64>], len: usize| -> Vec<Vec<f64>> {
        let mean: Vec<f64> = (0..len)
            .map(|c| rows.iter().map(|r| r[c]).sum::<f64>() / j as f64)
            .collect();
        rows.iter()
            .map(|r| r.iter().zip(&mean).map(|(v, m)| v - m).collect())
            .collect()
    };
    let (xc, yc) = (centre(pre, t0), centre(post, t1));
    let mut k: Vec<Vec<f64>> = xc
        .iter()
        .map(|a| xc.iter().map(|b| dot(a, b)).collect())
        .collect();
    let lambda = (0..j).map(|i| k[i][i]).sum::<f64>() / j as f64;
    if lambda <= 0.0 {
        return vec![0.0; t1];
    }
    for (i, row) in k.iter_mut().enumerate() {
        row[i] += lambda;
    }
    // rᵀ Xcᵀ (XcXcᵀ + λI)⁻¹ Yc: one J-vector solve serves every post day.
    let rx: Vec<f64> = xc.iter().map(|row| dot(row, residual)).collect();
    let Some(alpha) = solve_spd(&k, &rx) else {
        return vec![0.0; t1];
    };
    (0..t1)
        .map(|t| (0..j).map(|i| alpha[i] * yc[i][t]).sum())
        .collect()
}

/// Days held out at the end of the pre window: as long as the post window,
/// but never more than a third of the pre window.
fn holdout_days(pre: (usize, usize), post: (usize, usize)) -> usize {
    (post.1 - post.0).min((pre.1 - pre.0) / 3).max(1)
}

pub(crate) fn fit(input: &FitInput) -> Fit {
    let h = holdout_days(input.pre, input.post);
    let inner = FitInput {
        pre: (input.pre.0, input.pre.1 - h),
        post: (input.pre.1 - h, input.pre.1),
        ..*input
    };
    let holdout = project(&inner).gaps;
    let mut f = project(input).into_fit();
    f.holdout_rmspe = rms(&holdout);
    f
}

struct Projected {
    fit: Fit,
    gaps: Vec<f64>,
}

impl Projected {
    fn into_fit(self) -> Fit {
        self.fit
    }
}

fn project(input: &FitInput) -> Projected {
    let FitInput {
        m,
        unit,
        donors,
        pre,
        post,
    } = *input;
    let (level, own) = demeaned(m, unit, pre);
    let rows: Vec<Vec<f64>> = donors.iter().map(|d| demeaned(m, *d, pre).1).collect();
    let pre_of = |r: &[f64]| r[pre.0..pre.1].to_vec();
    let post_of = |r: &[f64]| r[post.0..post.1].to_vec();
    let donor_pre: Vec<Vec<f64>> = rows.iter().map(|r| pre_of(r)).collect();
    let donor_post: Vec<Vec<f64>> = rows.iter().map(|r| post_of(r)).collect();
    let target = pre_of(&own);
    let weights = simplex_weights(&target, &donor_pre);
    let combine = |cols: &[Vec<f64>], len: usize| -> Vec<f64> {
        (0..len)
            .map(|t| weights.iter().zip(cols).map(|(w, c)| w * c[t]).sum())
            .collect()
    };
    let synth_pre = combine(&donor_pre, target.len());
    let residual: Vec<f64> = target.iter().zip(&synth_pre).map(|(a, s)| a - s).collect();
    let correction = ridge_correction(&residual, &donor_pre, &donor_post);
    let synth_post: Vec<f64> = combine(&donor_post, post.1 - post.0)
        .into_iter()
        .zip(&correction)
        .map(|(s, c)| s + c)
        .collect();
    let gaps: Vec<f64> = post_of(&own)
        .iter()
        .zip(&synth_post)
        .map(|(a, s)| a - s)
        .collect();
    let fit = Fit {
        effect: gaps.iter().sum::<f64>() / gaps.len() as f64,
        pre_rmspe: rms(&residual),
        pre_sd: rms(&target),
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
        let w = simplex_weights(&target, &[a, b, c]);
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
}
