//! The two designs behind one entry point, so the ratio and the host never
//! branch: waves (common date or staggered, optionally stratified) and the
//! paired switchback.

use crate::engine::experiment::estimate::{
    decide, estimate_effect, Assignment, Decision, EffectResult,
};
use crate::engine::experiment::switchback::{
    decide_switchback, estimate_switchback, SwitchbackSchedule,
};
use crate::engine::experiment::PanelMatrix;

#[derive(Debug, Clone, serde::Serialize)]
pub enum ExperimentDesign {
    Waves(Assignment),
    Switchback(SwitchbackSchedule),
}

pub fn estimate(m: &PanelMatrix, d: &ExperimentDesign, seed: u64) -> EffectResult {
    match d {
        ExperimentDesign::Waves(a) => estimate_effect(m, a, seed),
        ExperimentDesign::Switchback(s) => estimate_switchback(m, s, seed),
    }
}

/// Exactly the decision `estimate` reports, without its interval.
pub(crate) fn decide_design(m: &PanelMatrix, d: &ExperimentDesign, seed: u64) -> Decision {
    match d {
        ExperimentDesign::Waves(a) => decide(m, a, seed),
        ExperimentDesign::Switchback(s) => decide_switchback(m, s, seed),
    }
}

/// The same design registered as a single outcome: the first stage's test.
pub(crate) fn with_family_one(d: &ExperimentDesign) -> ExperimentDesign {
    match d {
        ExperimentDesign::Waves(a) => ExperimentDesign::Waves(Assignment {
            family: 1,
            ..a.clone()
        }),
        ExperimentDesign::Switchback(s) => ExperimentDesign::Switchback(SwitchbackSchedule {
            family: 1,
            ..s.clone()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::experiment::estimate::estimate_effect;
    use crate::engine::experiment::switchback::{estimate_switchback, propose_switchback};
    use crate::engine::experiment::testkit::{
        inject_switchback, noisy_panel, schedule, staggered_fixture,
    };

    fn switchback(effect: f64) -> (PanelMatrix, SwitchbackSchedule) {
        let s = schedule(propose_switchback(200, 7, 8, 4), 7, 2);
        (
            inject_switchback(&noisy_panel(24, 400, 4), &s, effect, 0),
            s,
        )
    }

    #[test]
    fn experiment_estimate_routes_each_design_to_its_estimator() {
        let (m, a) = staggered_fixture(25.0, 7);
        assert_eq!(
            format!("{:?}", estimate(&m, &ExperimentDesign::Waves(a.clone()), 3)),
            format!("{:?}", estimate_effect(&m, &a, 3))
        );
        let (m, s) = switchback(20.0);
        assert_eq!(
            format!(
                "{:?}",
                estimate(&m, &ExperimentDesign::Switchback(s.clone()), 3)
            ),
            format!("{:?}", estimate_switchback(&m, &s, 3))
        );
    }

    /// The decision the ratio inverts is the decision `estimate` reports, for
    /// both designs.
    #[test]
    fn experiment_decide_design_is_the_decision_estimate_reports() {
        let (mw, a) = staggered_fixture(25.0, 7);
        let (ms, s) = switchback(20.0);
        for (m, d) in [
            (mw, ExperimentDesign::Waves(a)),
            (ms, ExperimentDesign::Switchback(s)),
        ] {
            let r = estimate(&m, &d, 5);
            assert_eq!(
                decide_design(&m, &d, 5),
                Decision::Tested {
                    significant: r.significant,
                    p_value: r.p_value,
                    estimate: r.estimate,
                },
                "design {}",
                r.design
            );
        }
    }

    #[test]
    fn experiment_with_family_one_registers_the_first_stage_alone() {
        let (_, mut a) = staggered_fixture(25.0, 7);
        a.family = 3;
        let ExperimentDesign::Waves(w) = with_family_one(&ExperimentDesign::Waves(a)) else {
            panic!("the design kind must not change")
        };
        assert_eq!(w.family, 1);
        let (_, mut s) = switchback(0.0);
        s.family = 3;
        let ExperimentDesign::Switchback(x) = with_family_one(&ExperimentDesign::Switchback(s))
        else {
            panic!("the design kind must not change")
        };
        assert_eq!(x.family, 1);
    }
}
