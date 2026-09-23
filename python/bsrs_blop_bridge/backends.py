"""Optimizer factories the bridge can serve.

``random``
    A self-contained baseline: uniform samples over the search space,
    best point by the objective expression. Useful to exercise the bridge
    without Ax, and as a random-search reference.
``ax``
    ``blop.ax.optimizer.AxOptimizer`` configured from the spec; needs
    ``pip install blop[ax]``.
"""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from typing import Any

from .server import ID_KEY

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
        self._next_id = 0
        self._params_by_id: dict[Any, dict[str, Any]] = {}
        self._outcomes_by_id: dict[Any, dict[str, Any]] = {}

    def _sample(self) -> dict[str, Any]:
        point = {}
        for p in self._parameters:
            if p["kind"] == "range":
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


BACKENDS = {"random": make_random, "ax": make_ax}
