"""Optimizer factories the bridge can serve.

``random``
    A self-contained baseline: uniform samples over the search space,
    best point by the objective expression. Useful to exercise the bridge
    without Ax, and as a random-search reference.
``ax``
    ``blop.ax.optimizer.AxOptimizer`` configured from the spec; needs
    ``pip install blop[ax]``.
``xopt``
    ``blop.xopt.optimizer.XoptOptimizer`` around one Xopt generator built
    from the spec's ``VOCS``; needs ``pip install blop[xopt]``.
"""

from __future__ import annotations

import random
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .server import ID_KEY, Backend

Spec = Mapping[str, Any]


def parse_objectives(objective: str) -> list[tuple[str, bool]]:
    """``"intensity, -width"`` → ``[("intensity", False), ("width", True)]``.

    Only the comma-separated form blop renders for a list of objectives;
    a scalarized expression is refused.
    """
    objectives = []
    for term in objective.split(","):
        term = term.strip()
        minimize = term.startswith("-")
        name = term[1:].strip() if minimize else term
        if not name.replace("_", "").isalnum():
            raise ValueError(f"cannot parse objective term {term!r}: expected '<name>' or '-<name>'")
        objectives.append((name, minimize))
    return objectives


class RandomOptimizer:
    """Uniform random search over an ``ExperimentSpec``."""

    def __init__(self, spec: Spec, seed: int | None = None) -> None:
        self._parameters = list(spec["parameters"])
        self._objectives = parse_objectives(spec["objective"])
        self._rng = random.Random(seed)
        self._fixed: dict[str, Any] | None = None
        self._next_id = 0
        self._params_by_id: dict[Any, dict[str, Any]] = {}
        self._outcomes_by_id: dict[Any, dict[str, Any]] = {}

    @property
    def fixed_parameters(self) -> dict[str, Any] | None:
        """Parameters held at a value for future suggestions."""
        return self._fixed

    @fixed_parameters.setter
    def fixed_parameters(self, fixed: Mapping[str, Any] | None) -> None:
        if not fixed:
            self._fixed = None
            return
        unknown = set(fixed) - {p["name"] for p in self._parameters}
        if unknown:
            raise KeyError(f"Unknown parameter(s): {sorted(unknown)}")
        self._fixed = dict(fixed)

    def reconfigure_search_space(self, updates: Mapping[str, tuple[float, float] | list]) -> None:
        """Replace bounds (range) or values (choice); all or nothing."""
        by_name = {p["name"]: p for p in self._parameters}
        new = {}
        for name, update in updates.items():
            p = dict(by_name[name])
            if p["kind"] == "range":
                if not isinstance(update, tuple):
                    raise TypeError(f"range parameter {name!r} takes (lower, upper), got {update!r}")
                p["bounds"] = list(update)
            else:
                if not isinstance(update, list):
                    raise TypeError(f"choice parameter {name!r} takes a list of values, got {update!r}")
                p["values"] = list(update)
            new[name] = p
        self._parameters = [new.get(p["name"], p) for p in self._parameters]

    def _sample(self) -> dict[str, Any]:
        point = {}
        for p in self._parameters:
            if self._fixed and p["name"] in self._fixed:
                point[p["name"]] = self._fixed[p["name"]]
            elif p["kind"] == "range":
                lo, hi = p["bounds"]
                value = self._rng.uniform(lo, hi)
                step = p.get("step_size")
                if step:
                    value = lo + round((value - lo) / step) * step
                if p["parameter_type"] == "int":
                    value = int(round(value))
                point[p["name"]] = value
            else:
                point[p["name"]] = self._rng.choice(p["values"])
        return point

    def suggest(self, num_points: int | None = None) -> Sequence[Mapping]:
        return self.register_suggestions([self._sample() for _ in range(num_points or 1)])

    def register_suggestions(self, suggestions: Sequence[Mapping]) -> Sequence[Mapping]:
        registered = []
        for suggestion in suggestions:
            trial_id = self._next_id
            self._next_id += 1
            params = {k: v for k, v in suggestion.items() if k != ID_KEY}
            self._params_by_id[trial_id] = params
            registered.append({ID_KEY: trial_id, **params})
        return registered

    def ingest(self, points: Sequence[Mapping]) -> None:
        names = {p["name"] for p in self._parameters}
        for point in points:
            trial_id = point.get(ID_KEY)
            if trial_id is None:
                trial_id = self._next_id
                self._next_id += 1
            params = {k: v for k, v in point.items() if k in names}
            self._params_by_id[trial_id] = {**self._params_by_id.get(trial_id, {}), **params}
            self._outcomes_by_id[trial_id] = {k: v for k, v in point.items() if k not in names and k != ID_KEY}

    def register_failures(self, suggestions: Sequence[Mapping]) -> None:
        for suggestion in suggestions:
            self._params_by_id.pop(suggestion.get(ID_KEY), None)

    def _score(self, outcomes: Mapping[str, Any]) -> float:
        total = 0.0
        for name, minimize in self._objectives:
            value = outcomes.get(name)
            if not isinstance(value, (int, float)):
                return float("-inf")
            total += -value if minimize else value
        return total

    def get_best_points(self) -> Sequence[tuple[Any, Mapping, Mapping]]:
        scored = [
            (self._score(outcomes), trial_id)
            for trial_id, outcomes in self._outcomes_by_id.items()
            if self._score(outcomes) > float("-inf")
        ]
        if not scored:
            return []
        _, best_id = max(scored)
        return [(best_id, self._params_by_id.get(best_id, {}), self._outcomes_by_id[best_id])]


def make_random(spec: Spec, backend: Mapping[str, Any]) -> RandomOptimizer:
    """Factory for the ``random`` backend; ``backend["seed"]`` seeds it."""
    return RandomOptimizer(spec, seed=backend.get("seed"))


def make_ax(spec: Spec, backend: Mapping[str, Any]) -> Any:
    """Factory for the ``ax`` backend.

    ``backend`` keys: ``checkpoint_path`` (str), ``client_kwargs`` (dict),
    and any further keys are passed to ``Client.configure_experiment``
    (``name``, ``description``, ...).
    """
    try:
        from ax import ChoiceParameterConfig, RangeParameterConfig
        from blop.ax.optimizer import AxOptimizer
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ImportError("the ax backend needs `pip install blop[ax]`") from e

    parameters = []
    for p in spec["parameters"]:
        if p["kind"] == "range":
            parameters.append(
                RangeParameterConfig(
                    name=p["name"],
                    bounds=tuple(p["bounds"]),
                    parameter_type=p["parameter_type"],
                    step_size=p.get("step_size"),
                    scaling=p.get("scaling"),
                )
            )
        elif p["kind"] == "choice":
            dependents = p.get("dependent_parameters")
            parameters.append(
                ChoiceParameterConfig(
                    name=p["name"],
                    values=list(p["values"]),
                    parameter_type=p["parameter_type"],
                    is_ordered=p.get("is_ordered"),
                    dependent_parameters={value: list(names) for value, names in dependents} if dependents else None,
                )
            )
        else:
            raise ValueError(f"unknown parameter kind {p['kind']!r}")
    backend = dict(backend)
    return AxOptimizer(
        parameters=parameters,
        objective=spec["objective"],
        parameter_constraints=list(spec.get("parameter_constraints") or []) or None,
        outcome_constraints=list(spec.get("outcome_constraints") or []) or None,
        checkpoint_path=backend.pop("checkpoint_path", None),
        client_kwargs=backend.pop("client_kwargs", None),
        **backend,
    )


def restore_ax(checkpoint_path: str, backend: Mapping[str, Any]) -> Any:
    """``AxOptimizer.from_checkpoint``; ``backend`` is unused, the file has it all."""
    try:
        from blop.ax.optimizer import AxOptimizer
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ImportError("the ax backend needs `pip install blop[ax]`") from e
    return AxOptimizer.from_checkpoint(checkpoint_path)


_OUTCOME_CONSTRAINT = re.compile(r"^\s*(\w+)\s*(<=|>=)\s*([-+]?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?)\s*$")


def parse_outcome_constraint(text: str) -> tuple[str, list[Any]]:
    """``"width <= 1.5"`` → ``("width", ["LESS_THAN", 1.5])``.

    Only the ``<name> <= <number>`` / ``<name> >= <number>`` form the agent
    renders for a single-outcome constraint; Xopt has no expression
    constraints, so anything else is refused.
    """
    m = _OUTCOME_CONSTRAINT.match(text)
    if m is None:
        raise ValueError(f"cannot express outcome constraint {text!r} in Xopt: expected '<name> <= <number>' or '<name> >= <number>'")
    name, op, bound = m.groups()
    return name, ["LESS_THAN" if op == "<=" else "GREATER_THAN", float(bound)]


def vocs_from_spec(spec: Spec) -> Any:
    """The Xopt ``VOCS`` for an ``ExperimentSpec``.

    A range parameter is a continuous variable over its bounds (Xopt has no
    integer variables; ``step_size`` is dropped), a choice parameter with
    numeric values is a discrete variable, and a choice parameter with
    string values is refused, since Xopt samples every variable as a
    float. Parameter constraints are refused: Xopt has none.
    """
    from gest_api.vocs import DiscreteVariable
    from xopt.vocs import VOCS

    variables: dict[str, Any] = {}
    for p in spec["parameters"]:
        if p["kind"] == "range":
            lo, hi = p["bounds"]
            variables[p["name"]] = [float(lo), float(hi)]
        elif p["kind"] == "choice":
            values = list(p["values"])
            if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
                raise ValueError(f"choice parameter {p['name']!r} has non-numeric values; Xopt variables are numeric")
            variables[p["name"]] = DiscreteVariable(values=values)
        else:
            raise ValueError(f"unknown parameter kind {p['kind']!r}")
    if spec.get("parameter_constraints"):
        raise ValueError(f"Xopt has no parameter constraints; got {list(spec['parameter_constraints'])}")
    objectives = {name: "MINIMIZE" if minimize else "MAXIMIZE" for name, minimize in parse_objectives(spec["objective"])}
    constraints = dict(parse_outcome_constraint(c) for c in spec.get("outcome_constraints") or [])
    return VOCS(variables=variables, objectives=objectives, constraints=constraints)


def make_xopt(spec: Spec, backend: Mapping[str, Any]) -> Any:
    """Factory for the ``xopt`` backend.

    ``backend`` keys: ``generator`` (an Xopt generator name, default
    ``"random"``; ``expected_improvement``, ``upper_confidence_bound``, ...),
    ``generator_kwargs`` (dict, passed to the generator class) and
    ``checkpoint_path`` (str; Xopt's is fixed at construction).
    """
    try:
        from blop.xopt.optimizer import XoptOptimizer
        from xopt.generators import get_generator
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ImportError("the xopt backend needs `pip install blop[xopt]`") from e

    backend = dict(backend)
    generator_class = get_generator(backend.pop("generator", "random"))
    generator = generator_class(vocs=vocs_from_spec(spec), **(backend.pop("generator_kwargs", None) or {}))
    checkpoint_path = backend.pop("checkpoint_path", None)
    if backend:
        raise ValueError(f"unknown xopt backend option(s): {sorted(backend)}")
    return XoptOptimizer(generator, checkpoint_path=checkpoint_path)


def restore_xopt(checkpoint_path: str, backend: Mapping[str, Any]) -> Any:
    """``XoptOptimizer.from_checkpoint``; the file names the generator class."""
    try:
        from blop.xopt.optimizer import XoptOptimizer
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ImportError("the xopt backend needs `pip install blop[xopt]`") from e
    return XoptOptimizer.from_checkpoint(checkpoint_path)


BACKENDS = {
    "random": Backend(make_random),
    "ax": Backend(make_ax, restore_ax),
    "xopt": Backend(make_xopt, restore_xopt),
}
