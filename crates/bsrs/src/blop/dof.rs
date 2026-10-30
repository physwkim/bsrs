//! Degrees of freedom — bsrs's port of `blop.ax.dof`.
//!
//! A DOF is one input of the search space: a continuous [`RangeDof`] or a
//! discrete [`ChoiceDof`], bound to the device that realizes it (or to a name,
//! when something outside the RunEngine sets it). [`Dof::to_parameter_config`]
//! renders the backend-neutral [`ParameterConfig`] an optimizer is configured
//! with; its fields are those of Ax's `RangeParameterConfig` /
//! `ChoiceParameterConfig`, so a bridge can pass it on unchanged.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::error::{BsrsError, Result};

use super::expr;
use super::protocol::Actuator;

/// The value type of a choice DOF — Ax's `parameter_type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterType {
    /// Floating-point values.
    Float,
    /// Integer values.
    Int,
    /// String values.
    Str,
    /// Boolean values.
    Bool,
}

/// The value type of a range DOF: continuous or integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RangeType {
    /// Any value within the bounds.
    Float,
    /// Integer values within the bounds.
    Int,
}

/// How a range DOF is scaled for the model; `Log` for a parameter spanning
/// orders of magnitude.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scaling {
    /// Linear scaling.
    Linear,
    /// Logarithmic scaling.
    Log,
}

/// What realizes a DOF's value. blop's `DOF` takes `name` and `actuator`
/// as two optional fields and rejects the combinations that make no sense;
/// here each legal combination is a variant.
#[derive(Clone, Debug)]
pub enum DofBinding {
    /// A device the RunEngine moves during acquisition; the parameter is
    /// named after the device.
    Actuator(Actuator),
    /// A parameter something outside the RunEngine controls, known only
    /// by name.
    External(String),
    /// A device named by the variable a queueserver worker resolves it
    /// from, with the device's own name as the parameter name when it
    /// differs. Not usable by an in-process [`Agent`](super::agent::Agent).
    Remote {
        /// The variable name the worker resolves the device from.
        actuator: String,
        /// The device's name; `None` when it is the variable name.
        name: Option<String>,
    },
}

impl DofBinding {
    /// The parameter name the optimizer sees.
    pub fn parameter_name(&self) -> &str {
        match self {
            DofBinding::Actuator(a) => a.name(),
            DofBinding::External(name) => name,
            DofBinding::Remote { actuator, name } => name.as_deref().unwrap_or(actuator),
        }
    }

    /// The in-process actuator, if the DOF is bound to one.
    pub fn actuator(&self) -> Option<&Actuator> {
        match self {
            DofBinding::Actuator(a) => Some(a),
            _ => None,
        }
    }
}

impl From<Actuator> for DofBinding {
    fn from(a: Actuator) -> Self {
        DofBinding::Actuator(a)
    }
}

impl From<&str> for DofBinding {
    fn from(name: &str) -> Self {
        DofBinding::External(name.to_owned())
    }
}

impl From<String> for DofBinding {
    fn from(name: String) -> Self {
        DofBinding::External(name)
    }
}

/// A continuous (or integer) DOF within bounds — blop's `RangeDOF`.
#[derive(Clone, Debug)]
pub struct RangeDof {
    /// What realizes the value.
    pub binding: DofBinding,
    /// The search domain, `(lower, upper)`.
    pub bounds: (f64, f64),
    /// Continuous or integer.
    pub parameter_type: RangeType,
    /// Suggestions are multiples of this step, when set.
    pub step_size: Option<f64>,
    /// Model scaling of the parameter.
    pub scaling: Option<Scaling>,
}

impl RangeDof {
    /// A range DOF with no step size and default scaling.
    pub fn new(
        binding: impl Into<DofBinding>,
        bounds: (f64, f64),
        parameter_type: RangeType,
    ) -> Self {
        Self {
            binding: binding.into(),
            bounds,
            parameter_type,
            step_size: None,
            scaling: None,
        }
    }

    /// Set the step size.
    pub fn step_size(mut self, step: f64) -> Self {
        self.step_size = Some(step);
        self
    }

    /// Set the scaling.
    pub fn scaling(mut self, scaling: Scaling) -> Self {
        self.scaling = Some(scaling);
        self
    }
}

/// A DOF taking one of a fixed set of values — blop's `ChoiceDOF`.
#[derive(Clone, Debug)]
pub struct ChoiceDof {
    /// What realizes the value.
    pub binding: DofBinding,
    /// The possible values.
    pub values: Vec<Value>,
    /// The type of the values.
    pub parameter_type: ParameterType,
    /// Whether the values are ordered; inferred from them when `None`.
    pub is_ordered: Option<bool>,
    /// Which other DOFs are active for each value of this one.
    pub dependent_parameters: Option<Vec<(Value, Vec<String>)>>,
}

impl ChoiceDof {
    /// A choice DOF with ordering inferred and no dependent parameters.
    pub fn new<V: Into<Value>>(
        binding: impl Into<DofBinding>,
        values: impl IntoIterator<Item = V>,
        parameter_type: ParameterType,
    ) -> Self {
        Self {
            binding: binding.into(),
            values: values.into_iter().map(Into::into).collect(),
            parameter_type,
            is_ordered: None,
            dependent_parameters: None,
        }
    }

    /// State whether the values are ordered.
    pub fn is_ordered(mut self, ordered: bool) -> Self {
        self.is_ordered = Some(ordered);
        self
    }

    /// Set which other DOFs are active for each value.
    pub fn dependent_parameters(
        mut self,
        dependents: impl IntoIterator<Item = (Value, Vec<String>)>,
    ) -> Self {
        self.dependent_parameters = Some(dependents.into_iter().collect());
        self
    }
}

/// A degree of freedom — blop's `DOF`.
#[derive(Clone, Debug)]
pub enum Dof {
    /// A continuous or integer range.
    Range(RangeDof),
    /// A discrete choice.
    Choice(ChoiceDof),
}

impl From<RangeDof> for Dof {
    fn from(d: RangeDof) -> Self {
        Dof::Range(d)
    }
}

impl From<ChoiceDof> for Dof {
    fn from(d: ChoiceDof) -> Self {
        Dof::Choice(d)
    }
}

impl Dof {
    /// What realizes the value.
    pub fn binding(&self) -> &DofBinding {
        match self {
            Dof::Range(d) => &d.binding,
            Dof::Choice(d) => &d.binding,
        }
    }

    /// The parameter name the optimizer sees.
    pub fn parameter_name(&self) -> &str {
        self.binding().parameter_name()
    }

    /// The in-process actuator, if the DOF is bound to one.
    pub fn actuator(&self) -> Option<&Actuator> {
        self.binding().actuator()
    }

    /// The parameter configuration an optimizer is set up with.
    pub fn to_parameter_config(&self) -> ParameterConfig {
        let name = self.parameter_name().to_owned();
        match self {
            Dof::Range(d) => ParameterConfig::Range {
                name,
                bounds: d.bounds,
                parameter_type: d.parameter_type,
                step_size: d.step_size,
                scaling: d.scaling,
            },
            Dof::Choice(d) => ParameterConfig::Choice {
                name,
                values: d.values.clone(),
                parameter_type: d.parameter_type,
                is_ordered: d.is_ordered,
                dependent_parameters: d.dependent_parameters.clone(),
            },
        }
    }
}

/// A backend-neutral parameter description: the fields of Ax's
/// `RangeParameterConfig` and `ChoiceParameterConfig`, tagged by `kind`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParameterConfig {
    /// A continuous or integer range.
    Range {
        /// Parameter name.
        name: String,
        /// `(lower, upper)`.
        bounds: (f64, f64),
        /// Continuous or integer.
        parameter_type: RangeType,
        /// Suggestions are multiples of this step, when set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_size: Option<f64>,
        /// Model scaling.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scaling: Option<Scaling>,
    },
    /// A discrete choice.
    Choice {
        /// Parameter name.
        name: String,
        /// The possible values.
        values: Vec<Value>,
        /// The type of the values.
        parameter_type: ParameterType,
        /// Whether the values are ordered.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        is_ordered: Option<bool>,
        /// Which other parameters are active for each value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dependent_parameters: Option<Vec<(Value, Vec<String>)>>,
    },
}

impl ParameterConfig {
    /// The parameter name.
    pub fn name(&self) -> &str {
        match self {
            ParameterConfig::Range { name, .. } | ParameterConfig::Choice { name, .. } => name,
        }
    }
}

/// An inequality over DOFs that refines the search space — blop's
/// `DOFConstraint`. The template names DOFs by variables of the caller's
/// choosing (`"x + y <= 12"` with `x` and `y` mapped to DOFs), and
/// [`expression`](Self::expression) renders it with the parameter names.
#[derive(Clone, Debug)]
pub struct DofConstraint {
    constraint: String,
    dofs: Vec<(String, Dof)>,
}

impl DofConstraint {
    /// Map each template variable of `constraint` to a DOF. Every variable
    /// must occur in the template as a whole word.
    pub fn new<K: Into<String>>(
        constraint: impl Into<String>,
        dofs: impl IntoIterator<Item = (K, Dof)>,
    ) -> Result<Self> {
        let constraint = constraint.into();
        let dofs: Vec<(String, Dof)> = dofs.into_iter().map(|(k, d)| (k.into(), d)).collect();
        if dofs.is_empty() {
            return Err(BsrsError::Plan(
                "DofConstraint requires at least one DOF to be specified. Map template \
                 variables to DOFs: DofConstraint::new(\"x + y <= 12\", [(\"x\", dof_x), \
                 (\"y\", dof_y)]). The variable names (x, y) are your choice and make the \
                 constraint readable."
                    .into(),
            ));
        }
        let missing: Vec<String> = dofs
            .iter()
            .filter(|(k, _)| !expr::contains_word(&constraint, k))
            .map(|(k, d)| format!("{k}={}", d.parameter_name()))
            .collect();
        if !missing.is_empty() {
            return Err(BsrsError::Plan(format!(
                "The following DOFs did not have matching names in the constraint '{constraint}': {}",
                missing.join(", ")
            )));
        }
        Ok(Self { constraint, dofs })
    }

    /// The constraint with parameter names in place of the variables.
    pub fn expression(&self) -> String {
        expr::substitute(&self.constraint, |w| {
            self.dofs
                .iter()
                .find(|(k, _)| k == w)
                .map(|(_, d)| d.parameter_name().to_owned())
        })
    }

    /// The DOFs the constraint ranges over, by template variable.
    pub fn dofs(&self) -> &[(String, Dof)] {
        &self.dofs
    }
}

impl fmt::Display for DofConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.expression())
    }
}

#[cfg(test)]
mod tests {
    //! Ports of blop's `tests/ax/test_dof.py`. `test_dof_invalid` has no
    //! counterpart: the binding enum cannot express an actuator with a
    //! separate name, nor a DOF with neither.

    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::backends::soft::SoftMotor;

    fn movable(name: &str) -> Actuator {
        Actuator::new(Arc::new(SoftMotor::new(name, Some(0.0))))
    }

    fn range(binding: impl Into<DofBinding>) -> RangeDof {
        RangeDof::new(binding, (0.0, 1.0), RangeType::Float)
            .step_size(0.1)
            .scaling(Scaling::Linear)
    }

    fn choice(binding: impl Into<DofBinding>) -> ChoiceDof {
        ChoiceDof::new(binding, [0, 1, 2, 3, 4, 5], ParameterType::Int).is_ordered(true)
    }

    #[test]
    fn dof_movable() {
        assert_eq!(
            range(movable("test_movable")).binding.parameter_name(),
            "test_movable"
        );
        assert_eq!(
            Dof::from(choice(movable("test_movable"))).parameter_name(),
            "test_movable"
        );
    }

    #[test]
    fn dof_name() {
        assert_eq!(Dof::from(range("test_name")).parameter_name(), "test_name");
        assert_eq!(Dof::from(choice("test_name")).parameter_name(), "test_name");
        assert!(Dof::from(range("test_name")).actuator().is_none());
    }

    #[test]
    fn dof_actuator_and_name() {
        // A queueserver worker resolves `test.sub1`; the optimizer sees the
        // device's own name.
        let remote = DofBinding::Remote {
            actuator: "test.sub1".into(),
            name: Some("test_sub1".into()),
        };
        assert_eq!(Dof::from(range(remote)).parameter_name(), "test_sub1");
        let unnamed = DofBinding::Remote {
            actuator: "test.sub2".into(),
            name: None,
        };
        assert_eq!(Dof::from(choice(unnamed)).parameter_name(), "test.sub2");
    }

    #[test]
    fn range_dof() {
        let expected = |name: &str| ParameterConfig::Range {
            name: name.into(),
            bounds: (0.0, 1.0),
            parameter_type: RangeType::Float,
            step_size: Some(0.1),
            scaling: Some(Scaling::Linear),
        };
        assert_eq!(
            Dof::from(range("test_name")).to_parameter_config(),
            expected("test_name")
        );
        assert_eq!(
            Dof::from(range(movable("test_movable"))).to_parameter_config(),
            expected("test_movable")
        );
    }

    #[test]
    fn choice_dof() {
        let expected = |name: &str| ParameterConfig::Choice {
            name: name.into(),
            values: vec![json!(0), json!(1), json!(2), json!(3), json!(4), json!(5)],
            parameter_type: ParameterType::Int,
            is_ordered: Some(true),
            dependent_parameters: None,
        };
        assert_eq!(
            Dof::from(choice("test_name")).to_parameter_config(),
            expected("test_name")
        );
        assert_eq!(
            Dof::from(choice(movable("test_movable"))).to_parameter_config(),
            expected("test_movable")
        );
    }

    #[test]
    fn parameter_config_serializes_with_ax_field_names() {
        let cfg = Dof::from(range("x")).to_parameter_config();
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            json!({"kind": "range", "name": "x", "bounds": [0.0, 1.0], "parameter_type": "float",
                   "step_size": 0.1, "scaling": "linear"})
        );
        let cfg = Dof::from(ChoiceDof::new("f", ["none", "Al"], ParameterType::Str))
            .to_parameter_config();
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            json!({"kind": "choice", "name": "f", "values": ["none", "Al"], "parameter_type": "str"})
        );
        let back: ParameterConfig =
            serde_json::from_value(serde_json::to_value(&cfg).unwrap()).unwrap();
        assert_eq!(back, cfg);
    }

    #[test]
    fn dof_constraint() {
        let c = DofConstraint::new(
            "x1 + x2 <= 10",
            [
                ("x1", Dof::from(range("test_dof1"))),
                ("x2", Dof::from(range("test_dof2"))),
            ],
        )
        .unwrap();
        assert_eq!(c.expression(), "test_dof1 + test_dof2 <= 10");
        assert_eq!(c.to_string(), "test_dof1 + test_dof2 <= 10");
    }

    #[test]
    fn invalid_dof_constraint() {
        let err = DofConstraint::new(
            "x1 + x2 <= 10",
            [
                ("x1", Dof::from(range("test_dof1"))),
                ("x3", Dof::from(range("test_dof2"))),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("x3=test_dof2"), "{err}");
        let err = DofConstraint::new("x1 <= 10", Vec::<(String, Dof)>::new()).unwrap_err();
        assert!(err.to_string().contains("at least one DOF"), "{err}");
    }

    #[test]
    fn dof_constraint_variable_must_be_a_whole_word() {
        // blop's substring check accepts `x` here and then substitutes
        // nothing; the whole-word check refuses it up front.
        let err = DofConstraint::new("x1 + x2 <= 10", [("x", Dof::from(range("d")))]).unwrap_err();
        assert!(err.to_string().contains("x=d"), "{err}");
    }
}
