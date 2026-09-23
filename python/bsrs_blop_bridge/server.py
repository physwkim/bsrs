"""The REQ/REP server: one JSON request in, one JSON reply out.

Wire format
-----------
Request::

    {"method": "<name>", "params": {...}}

Reply::

    {"result": <value>}          on success
    {"error": "<message>"}       on failure (the optimizer is left as it was)

Methods, with ``params`` and ``result``:

``configure``
    ``{"spec": ExperimentSpec, "backend": {...}}`` → ``{"name": str,
    "capabilities": {"register_suggestions": bool, "stopping_criteria":
    bool, "checkpoint": bool}}``. ``spec`` is bsrs's ``ExperimentSpec``
    (``parameters``, ``objective``, ``parameter_constraints``,
    ``outcome_constraints``); ``backend`` is passed to the optimizer
    factory. Reconfiguring replaces the optimizer.
``suggest``
    ``{"n": int}`` → ``[{"_id": id, <param>: value, ...}, ...]``
``ingest``
    ``{"points": [{"_id": id, <outcome>: value, ...}, ...]}`` → ``null``
``best_points``
    ``{}`` → ``[{"_id": id, "params": {...}, "outcomes": {...}}, ...]``.
    An outcome value is whatever the optimizer reports; Ax reports
    ``[mean, sem]`` pairs.
``register_suggestions``
    ``{"suggestions": [{<param>: value, ...}, ...]}`` → suggestions with ids
``register_failures``
    ``{"suggestions": [{"_id": id, ...}, ...]}`` → ``null``
``should_stop``
    ``{}`` → ``[bool, reason | null]``
``checkpoint``
    ``{}`` → ``null``
``ping``
    ``{}`` → ``"pong"``

Values are JSON: numpy scalars are converted, NaN and infinities become
``null`` (bsrs's JSON parser has no NaN).
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections.abc import Callable, Mapping
from typing import Any

import zmq

logger = logging.getLogger("bsrs_blop_bridge")

ID_KEY = "_id"

OptimizerFactory = Callable[[Mapping[str, Any], Mapping[str, Any]], Any]


def _jsonable(value: Any) -> Any:
    """Turn numpy scalars/arrays and non-finite floats into JSON values."""
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def capabilities(optimizer: Any) -> dict[str, bool]:
    """Which optional blop protocols the optimizer implements."""
    return {
        "register_suggestions": callable(getattr(optimizer, "register_suggestions", None)),
        "stopping_criteria": callable(getattr(optimizer, "should_stop", None)),
        "checkpoint": callable(getattr(optimizer, "checkpoint", None)),
    }


class Server:
    """Dispatch bridge requests to one optimizer built by ``make_optimizer``.

    Parameters
    ----------
    make_optimizer
        ``(spec, backend) -> optimizer``; called on ``configure``.
    """

    def __init__(self, make_optimizer: OptimizerFactory) -> None:
        self._make_optimizer = make_optimizer
        self._optimizer: Any = None

    @property
    def optimizer(self) -> Any:
        """The optimizer of the last ``configure``, or ``None``."""
        return self._optimizer

    def handle(self, request: bytes) -> bytes:
        """One request frame in, one reply frame out; never raises."""
        try:
            message = json.loads(request)
            method = message["method"]
            params = message.get("params") or {}
            result = self._dispatch(method, params)
            return json.dumps({"result": _jsonable(result)}).encode()
        except Exception as e:  # noqa: BLE001 - every failure becomes a reply
            logger.exception("bridge request failed")
            return json.dumps({"error": f"{type(e).__name__}: {e}"}).encode()

    def _require(self) -> Any:
        if self._optimizer is None:
            raise RuntimeError("no optimizer: send 'configure' first")
        return self._optimizer

    def _dispatch(self, method: str, params: Mapping[str, Any]) -> Any:
        if method == "ping":
            return "pong"
        if method == "configure":
            self._optimizer = self._make_optimizer(params["spec"], params.get("backend") or {})
            return {
                "name": type(self._optimizer).__name__,
                "capabilities": capabilities(self._optimizer),
            }
        optimizer = self._require()
        if method == "suggest":
            return list(optimizer.suggest(int(params.get("n", 1))))
        if method == "ingest":
            optimizer.ingest(list(params["points"]))
            return None
        if method == "best_points":
            return [
                {ID_KEY: trial_id, "params": dict(parameters), "outcomes": dict(outcomes)}
                for trial_id, parameters, outcomes in optimizer.get_best_points()
            ]
        if method == "register_suggestions":
            return list(optimizer.register_suggestions(list(params["suggestions"])))
        if method == "register_failures":
            optimizer.register_failures(list(params["suggestions"]))
            return None
        if method == "should_stop":
            stop, reason = optimizer.should_stop()
            return [bool(stop), reason]
        if method == "checkpoint":
            optimizer.checkpoint()
            return None
        raise ValueError(f"unknown method {method!r}")


def serve(
    endpoint: str,
    make_optimizer: OptimizerFactory,
    *,
    stop: threading.Event | None = None,
    ready: Callable[[str], None] | None = None,
    poll_ms: int = 200,
) -> None:
    """Bind a REP socket at ``endpoint`` and answer requests until ``stop`` is set.

    ``ready`` receives the bound endpoint (a wildcard port resolved) once
    the socket is listening.
    """
    server = Server(make_optimizer)
    context = zmq.Context.instance()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind(endpoint)
    bound = socket.getsockopt_string(zmq.LAST_ENDPOINT)
    logger.info("bsrs blop bridge listening at %s", bound)
    if ready is not None:
        ready(bound)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    try:
        while stop is None or not stop.is_set():
            if not poller.poll(poll_ms):
                continue
            socket.send(server.handle(socket.recv()))
    finally:
        socket.close()
