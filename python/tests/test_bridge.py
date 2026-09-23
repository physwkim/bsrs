"""The bridge server over the random backend, driven by a raw REQ client."""

import json
import threading

import pytest
import zmq

from bsrs_blop_bridge.backends import BACKENDS, parse_objectives
from bsrs_blop_bridge.server import Server, serve

SPEC = {
    "parameters": [
        {"kind": "range", "name": "x", "bounds": [0.0, 10.0], "parameter_type": "float"},
        {"kind": "range", "name": "n", "bounds": [1, 4], "parameter_type": "int", "step_size": 1},
        {"kind": "choice", "name": "filter", "values": ["none", "Al"], "parameter_type": "str"},
    ],
    "objective": "intensity, -width",
    "parameter_constraints": [],
    "outcome_constraints": [],
}


def call(server, method, **params):
    reply = json.loads(server.handle(json.dumps({"method": method, "params": params}).encode()))
    assert "error" not in reply, reply
    return reply["result"]


def test_configure_reports_name_and_capabilities():
    server = Server(BACKENDS["random"])
    result = call(server, "configure", spec=SPEC, backend={"seed": 1})
    assert result == {
        "name": "RandomOptimizer",
        "capabilities": {
            "register_suggestions": True,
            "stopping_criteria": False,
            "checkpoint": False,
            "checkpoint_path": False,
            "fixed_parameters": True,
            "reconfigure_search_space": True,
        },
    }


def test_suggest_ingest_best_points_roundtrip():
    server = Server(BACKENDS["random"])
    call(server, "configure", spec=SPEC, backend={"seed": 1})
    suggestions = call(server, "suggest", n=3)
    assert [s["_id"] for s in suggestions] == [0, 1, 2]
    for s in suggestions:
        assert 0.0 <= s["x"] <= 10.0
        assert s["n"] in {1, 2, 3, 4}
        assert s["filter"] in {"none", "Al"}
    call(
        server,
        "ingest",
        points=[
            {"_id": 0, "intensity": 1.0, "width": 5.0},
            {"_id": 1, "intensity": 3.0, "width": 1.0},
            {"_id": 2, "intensity": 2.0, "width": float("nan")},
        ],
    )
    best = call(server, "best_points")
    assert len(best) == 1
    assert best[0]["_id"] == 1
    assert best[0]["params"] == {k: v for k, v in suggestions[1].items() if k != "_id"}
    assert best[0]["outcomes"] == {"intensity": 3.0, "width": 1.0}


def test_errors_become_replies_and_leave_the_optimizer():
    server = Server(BACKENDS["random"])
    reply = json.loads(server.handle(json.dumps({"method": "suggest", "params": {"n": 1}}).encode()))
    assert reply["error"].startswith("RuntimeError: no optimizer")
    call(server, "configure", spec=SPEC, backend={})
    reply = json.loads(server.handle(json.dumps({"method": "nope"}).encode()))
    assert "unknown method" in reply["error"]
    reply = json.loads(server.handle(b"not json"))
    assert "JSONDecodeError" in reply["error"]
    assert server.optimizer is not None
    assert call(server, "ping") == "pong"


def test_parse_objectives():
    assert parse_objectives("intensity, -width") == [("intensity", False), ("width", True)]
    with pytest.raises(ValueError):
        parse_objectives("-(a + b)")


def test_serve_over_tcp():
    stop = threading.Event()
    bound = {}
    ready = threading.Event()

    def on_ready(endpoint):
        bound["endpoint"] = endpoint
        ready.set()

    thread = threading.Thread(
        target=serve, args=("tcp://127.0.0.1:*", BACKENDS["random"]), kwargs={"stop": stop, "ready": on_ready}, daemon=True
    )
    thread.start()
    assert ready.wait(5)
    client = zmq.Context.instance().socket(zmq.REQ)
    client.setsockopt(zmq.LINGER, 0)
    client.connect(bound["endpoint"])
    try:
        client.send_json({"method": "configure", "params": {"spec": SPEC, "backend": {"seed": 7}}})
        assert client.recv_json()["result"]["name"] == "RandomOptimizer"
        client.send_json({"method": "suggest", "params": {"n": 2}})
        assert len(client.recv_json()["result"]) == 2
    finally:
        client.close()
        stop.set()
        thread.join(5)


def test_ax_backend_optimizes_a_quadratic():
    pytest.importorskip("ax")
    server = Server(BACKENDS["ax"])
    spec = {
        "parameters": [
            {"kind": "range", "name": "x1", "bounds": [-5.0, 5.0], "parameter_type": "float"},
            {"kind": "range", "name": "x2", "bounds": [-5.0, 5.0], "parameter_type": "float"},
        ],
        "objective": "-loss",
        "parameter_constraints": [],
        "outcome_constraints": [],
    }
    configured = call(server, "configure", spec=spec, backend={})
    assert configured["name"] == "AxOptimizer"
    assert configured["capabilities"] == {
        "register_suggestions": True,
        "stopping_criteria": True,
        "checkpoint": True,
        "checkpoint_path": True,
        "fixed_parameters": True,
        "reconfigure_search_space": True,
    }
    for _ in range(6):
        suggestions = call(server, "suggest", n=2)
        assert 1 <= len(suggestions) <= 2  # Ax may generate fewer than asked
        call(
            server,
            "ingest",
            points=[{"_id": s["_id"], "loss": (s["x1"] - 1) ** 2 + (s["x2"] + 2) ** 2} for s in suggestions],
        )
    (best,) = call(server, "best_points")
    assert set(best["params"]) == {"x1", "x2"}
    mean, _sem = best["outcomes"]["loss"]
    assert mean < 25.0
    assert call(server, "should_stop") == [False, None]


def test_fixed_parameters_and_search_space_over_the_random_backend():
    server = Server(BACKENDS["random"])
    call(server, "configure", spec=SPEC, backend={"seed": 3})
    assert call(server, "fixed_parameters") is None
    call(server, "set_fixed_parameters", fixed={"n": 2, "filter": "Al"})
    assert call(server, "fixed_parameters") == {"n": 2, "filter": "Al"}
    for s in call(server, "suggest", n=5):
        assert (s["n"], s["filter"]) == (2, "Al")
    call(server, "set_fixed_parameters", fixed=None)
    assert call(server, "fixed_parameters") is None

    call(server, "reconfigure_search_space", updates={"x": {"bounds": [3.0, 6.0]}, "filter": {"values": ["Al"]}})
    for s in call(server, "suggest", n=20):
        assert 3.0 <= s["x"] <= 6.0
        assert s["filter"] == "Al"

    reply = json.loads(server.handle(json.dumps({"method": "set_fixed_parameters", "params": {"fixed": {"zz": 1}}}).encode()))
    assert "Unknown parameter" in reply["error"]
    reply = json.loads(server.handle(json.dumps({"method": "reconfigure_search_space", "params": {"updates": {"x": {"nope": 1}}}}).encode()))
    assert "needs 'bounds' or 'values'" in reply["error"]
    reply = json.loads(server.handle(json.dumps({"method": "restore", "params": {"checkpoint_path": "x.json"}}).encode()))
    assert "cannot restore" in reply["error"]
    reply = json.loads(server.handle(json.dumps({"method": "checkpoint_path"}).encode()))
    assert "AttributeError" in reply["error"]


def test_ax_checkpoint_restore_fixed_and_search_space(tmp_path):
    pytest.importorskip("ax")
    path = str(tmp_path / "checkpoint.json")
    spec = {
        "parameters": [
            {"kind": "range", "name": "x1", "bounds": [0.0, 10.0], "parameter_type": "float"},
            {"kind": "range", "name": "x2", "bounds": [0.0, 10.0], "parameter_type": "float"},
            {"kind": "choice", "name": "x3", "values": ["A", "B", "C", "D", "E"], "parameter_type": "str"},
        ],
        "objective": "obj",
        "parameter_constraints": [],
        "outcome_constraints": [],
    }
    server = Server(BACKENDS["ax"])
    call(server, "configure", spec=spec, backend={})
    assert call(server, "checkpoint_path") is None
    call(server, "set_checkpoint_path", path=path)
    assert call(server, "checkpoint_path") == path

    call(server, "set_fixed_parameters", fixed={"x2": 4, "x3": "B"})
    suggestions = call(server, "suggest", n=3)
    for s in suggestions[1:]:  # the first trial is the centre of the search space
        assert (s["x2"], s["x3"]) == (4, "B")
    call(server, "set_fixed_parameters", fixed=None)
    # Ax's Sobol node wants data before it generates more. The centre trial
    # gets the top value: it is the one point sure to stay inside the
    # narrowed space below, and Ax's best point is filtered to that space.
    call(server, "ingest", points=[{"_id": s["_id"], "obj": float(-i)} for i, s in enumerate(suggestions)])

    call(server, "reconfigure_search_space", updates={"x1": {"bounds": [3.0, 6.0]}, "x3": {"values": ["B", "C"]}})
    for s in call(server, "suggest", n=3):
        assert 3.0 <= s["x1"] <= 6.0
        assert s["x3"] in {"B", "C"}

    call(server, "checkpoint")
    assert (tmp_path / "checkpoint.json").exists()
    (best,) = call(server, "best_points")
    assert best["_id"] == suggestions[0]["_id"]

    restored = Server(BACKENDS["ax"])
    result = call(restored, "restore", checkpoint_path=path, backend={})
    assert result["name"] == "AxOptimizer"
    assert call(restored, "checkpoint_path") == path
    assert call(restored, "best_points") == [best]
