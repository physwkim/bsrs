//! Objectives and outcome constraints — bsrs's port of `blop.ax.objective`.
//!
//! An [`Objective`] names an outcome of the evaluation function and the
//! direction to drive it; a [`ScalarizedObjective`] combines several into
//! one expression; an [`OutcomeConstraint`] is a soft inequality on
//! outcomes. Each renders to the expression string an optimizer is
//! configured with (Ax's `objective` / `outcome_constraints`), with a
//! minimized objective negated.

use std::fmt;

use crate::core::error::{BsrsError, Result};

use super::expr;

/// A measurable outcome to drive up or down — blop's `Objective`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Objective {
    /// The outcome's key in what the evaluation function returns.
    pub name: String,
    /// Drive the outcome down (`true`) or up (`false`).
    pub minimize: bool,
}

impl Objective {
    /// An objective on `name`.
    pub fn new(name: impl Into<String>, minimize: bool) -> Self {
        Self {
            name: name.into(),
            minimize,
        }
    }

    /// An objective to minimize.
    pub fn minimize(name: impl Into<String>) -> Self {
        Self::new(name, true)
    }

    /// An objective to maximize.
    pub fn maximize(name: impl Into<String>) -> Self {
        Self::new(name, false)
    }

    /// The optimizer expression: the name, negated when minimizing.
    pub fn expression(&self) -> String {
        if self.minimize {
            format!("-{}", self.name)
        } else {
            self.name.clone()
        }
    }
}

/// Something an outcome constraint can range over: an [`Objective`] or a
/// tracked metric known by name.
pub trait OutcomeName {
    /// The outcome's name.
    fn outcome_name(&self) -> String;
}

impl OutcomeName for Objective {
    fn outcome_name(&self) -> String {
        self.name.clone()
    }
}

impl OutcomeName for &Objective {
    fn outcome_name(&self) -> String {
        self.name.clone()
    }
}

impl OutcomeName for &str {
    fn outcome_name(&self) -> String {
        (*self).to_owned()
    }
}

impl OutcomeName for String {
    fn outcome_name(&self) -> String {
        self.clone()
    }
}

/// A weighted combination of outcomes optimized as one — blop's
/// `ScalarizedObjective`. The expression names outcomes by variables of
/// the caller's choosing, mapped to outcome names.
#[derive(Clone, Debug)]
pub struct ScalarizedObjective {
    expression: String,
    minimize: bool,
    objectives: Vec<(String, String)>,
}

impl ScalarizedObjective {
    /// Map each variable of `expression` to an outcome name. Every
    /// variable must occur in the expression as a whole word.
    pub fn new<K: Into<String>, V: Into<String>>(
        expression: impl Into<String>,
        minimize: bool,
        objectives: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self> {
        let expression = expression.into();
        let objectives: Vec<(String, String)> = objectives
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if objectives.is_empty() {
            return Err(BsrsError::Plan(
                "ScalarizedObjective requires at least one objective.".into(),
            ));
        }
        let missing: Vec<&str> = objectives
            .iter()
            .filter(|(k, _)| !expr::contains_word(&expression, k))
            .map(|(k, _)| k.as_str())
            .collect();
        if !missing.is_empty() {
            return Err(BsrsError::Plan(format!(
                "Objectives {missing:?} not found in expression '{expression}'."
            )));
        }
        Ok(Self {
            expression,
            minimize,
            objectives,
        })
    }

    /// Whether the combination is minimized.
    pub fn minimize(&self) -> bool {
        self.minimize
    }

    /// The outcome names the expression ranges over.
    pub fn outcome_names(&self) -> Vec<String> {
        self.objectives.iter().map(|(_, v)| v.clone()).collect()
    }

    /// The optimizer expression, with outcome names substituted and the
    /// whole negated when minimizing.
    pub fn expression(&self) -> String {
        let body = expr::substitute(&self.expression, |w| {
            self.objectives
                .iter()
                .find(|(k, _)| k == w)
                .map(|(_, v)| v.clone())
        });
        if self.minimize {
            format!("-({body})")
        } else {
            body
        }
    }
}

impl fmt::Display for ScalarizedObjective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.expression())
    }
}

/// A soft inequality on outcomes — blop's `OutcomeConstraint`. It may be
/// violated while exploring and is satisfied increasingly as the
/// optimization progresses.
#[derive(Clone, Debug)]
pub struct OutcomeConstraint {
    constraint: String,
    outcomes: Vec<(String, String)>,
}

impl OutcomeConstraint {
    /// Map each variable of `constraint` to an objective or metric. Every
    /// variable must occur in the constraint as a whole word.
    pub fn new<K: Into<String>, V: OutcomeName>(
        constraint: impl Into<String>,
        outcomes: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self> {
        let constraint = constraint.into();
        let outcomes: Vec<(String, String)> = outcomes
            .into_iter()
            .map(|(k, v)| (k.into(), v.outcome_name()))
            .collect();
        if outcomes.is_empty() {
            return Err(BsrsError::Plan(
                "OutcomeConstraint requires at least one outcome.".into(),
            ));
        }
        let missing: Vec<&str> = outcomes
            .iter()
            .filter(|(k, _)| !expr::contains_word(&constraint, k))
            .map(|(k, _)| k.as_str())
            .collect();
        if !missing.is_empty() {
            return Err(BsrsError::Plan(format!(
                "Outcomes {missing:?} not found in constraint '{constraint}'."
            )));
        }
        Ok(Self {
            constraint,
            outcomes,
        })
    }

    /// The constraint with outcome names in place of the variables.
    pub fn expression(&self) -> String {
        expr::substitute(&self.constraint, |w| {
            self.outcomes
                .iter()
                .find(|(k, _)| k == w)
                .map(|(_, v)| v.clone())
        })
    }
}

impl fmt::Display for OutcomeConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.expression())
    }
}

/// What an agent optimizes: several objectives, or one scalarized
/// combination.
#[derive(Clone, Debug)]
pub enum Objectives {
    /// Each objective optimized in its own right (multi-objective when
    /// more than one).
    List(Vec<Objective>),
    /// One combined expression.
    Scalarized(ScalarizedObjective),
}

impl Objectives {
    /// The optimizer expression — blop's `to_ax_objective_str`: the
    /// objectives' expressions joined by `", "`, or the scalarized one.
    pub fn expression(&self) -> String {
        match self {
            Objectives::List(list) => objective_expression(list),
            Objectives::Scalarized(s) => s.expression(),
        }
    }

    /// The outcome names involved.
    pub fn outcome_names(&self) -> Vec<String> {
        match self {
            Objectives::List(list) => list.iter().map(|o| o.name.clone()).collect(),
            Objectives::Scalarized(s) => s.outcome_names(),
        }
    }
}

impl From<Vec<Objective>> for Objectives {
    fn from(list: Vec<Objective>) -> Self {
        Objectives::List(list)
    }
}

impl<const N: usize> From<[Objective; N]> for Objectives {
    fn from(list: [Objective; N]) -> Self {
        Objectives::List(list.into())
    }
}

impl From<Objective> for Objectives {
    fn from(o: Objective) -> Self {
        Objectives::List(vec![o])
    }
}

impl From<ScalarizedObjective> for Objectives {
    fn from(s: ScalarizedObjective) -> Self {
        Objectives::Scalarized(s)
    }
}

/// The optimizer expression of several objectives: each one's expression,
/// joined by `", "` — blop's `to_ax_objective_str` for a sequence.
pub fn objective_expression(objectives: &[Objective]) -> String {
    objectives
        .iter()
        .map(Objective::expression)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    //! Ports of blop's `tests/ax/test_objective.py`. The three
    //! `test_ax_api_consistency_*` cases exercise Ax itself and are not
    //! ported.

    use super::*;

    #[test]
    fn objective_expression_negates_when_minimizing() {
        assert_eq!(
            objective_expression(&[Objective::minimize("test_objective")]),
            "-test_objective"
        );
        assert_eq!(
            objective_expression(&[Objective::maximize("test_objective")]),
            "test_objective"
        );
    }

    #[test]
    fn multiple_objectives_join_with_commas() {
        let objectives = Objectives::from([
            Objective::minimize("test_objective1"),
            Objective::maximize("test_objective2"),
        ]);
        assert_eq!(objectives.expression(), "-test_objective1, test_objective2");
    }

    #[test]
    fn scalarized_objective_minimized_is_negated_as_a_whole() {
        let s = ScalarizedObjective::new(
            "x + y",
            true,
            [("x", "test_objective1"), ("y", "test_objective2")],
        )
        .unwrap();
        assert_eq!(s.expression(), "-(test_objective1 + test_objective2)");
        assert_eq!(s.to_string(), "-(test_objective1 + test_objective2)");
    }

    #[test]
    fn scalarized_objective_maximized_is_plain() {
        let s = ScalarizedObjective::new(
            "x + y",
            false,
            [("x", "test_objective1"), ("y", "test_objective2")],
        )
        .unwrap();
        assert_eq!(s.expression(), "test_objective1 + test_objective2");
        assert_eq!(
            Objectives::from(s).outcome_names(),
            ["test_objective1", "test_objective2"]
        );
    }

    #[test]
    fn scalarized_objective_rejects_unused_and_empty_variables() {
        let err = ScalarizedObjective::new("x + y", true, [("x", "a"), ("z", "b")]).unwrap_err();
        assert!(err.to_string().contains("[\"z\"]"), "{err}");
        let err = ScalarizedObjective::new("x", true, Vec::<(String, String)>::new()).unwrap_err();
        assert!(err.to_string().contains("at least one objective"), "{err}");
    }

    #[test]
    fn outcome_constraint_on_objective() {
        let objective = Objective::minimize("test_objective");
        let c = OutcomeConstraint::new("x >= 0.5", [("x", &objective)]).unwrap();
        assert_eq!(c.expression(), "test_objective >= 0.5");
    }

    #[test]
    fn outcome_constraint_on_metrics() {
        let c = OutcomeConstraint::new(
            "x + y >= 0.5",
            [("x", "test_metric1"), ("y", "test_metric2")],
        )
        .unwrap();
        assert_eq!(c.expression(), "test_metric1 + test_metric2 >= 0.5");
        assert_eq!(c.to_string(), "test_metric1 + test_metric2 >= 0.5");
    }

    #[test]
    fn outcome_constraint_rejects_unused_and_empty_variables() {
        let err = OutcomeConstraint::new("x >= 0.5", [("y", "m")]).unwrap_err();
        assert!(err.to_string().contains("[\"y\"]"), "{err}");
        let err = OutcomeConstraint::new("x >= 0.5", Vec::<(String, String)>::new()).unwrap_err();
        assert!(err.to_string().contains("at least one outcome"), "{err}");
    }
}
