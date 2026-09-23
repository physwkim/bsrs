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
        "capabilities": {"register_suggestions": True, "stopping_criteria": False, "checkpoint": False},
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
