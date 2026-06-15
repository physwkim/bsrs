"""Subscribe to bsrs Documents over ZMQ via bluesky's
RemoteDispatcher. Verifies wire-format compatibility — bsrs emits
the bluesky `Publisher` envelope (msgpack body), Python consumes it
unchanged.

Run:
    /Users/stevek/mamba/envs/bs2026.1/bin/python \\
        examples/mini_beamline/05_remote_dispatcher.py \\
        tcp://localhost:5577
"""

from __future__ import annotations

import sys
import threading

import msgpack
from bluesky.callbacks.zmq import RemoteDispatcher


def main(host: str, port: int) -> int:
    seen = {"start": 0, "descriptor": 0, "event": 0, "stop": 0, "other": 0}
    last_stop_uid: dict[str, str | None] = {"uid": None}
    done = threading.Event()

    def on_doc(name: str, doc: dict) -> None:
        seen[name] = seen.get(name, 0) + 1
        if name == "start":
            print(f"[py] start uid={doc.get('uid')[:8]} plan={doc.get('plan_name')}")
        elif name == "event":
            data = doc.get("data", {})
            print(f"[py]   event seq={doc.get('seq_num')} data={data}")
        elif name == "stop":
            last_stop_uid["uid"] = doc.get("run_start")
            print(f"[py] stop  exit_status={doc.get('exit_status')} run={doc.get('run_start')[:8]}")
            done.set()

    disp = RemoteDispatcher((host, port), deserializer=msgpack.unpackb)
    disp.subscribe(on_doc)
    print(f"[py] subscribed to {host}:{port}, waiting for documents...")

    t = threading.Thread(target=disp.start, daemon=True)
    t.start()

    # Wait for one full run (start → ... → stop).
    if not done.wait(timeout=60.0):
        print("[py] TIMEOUT — no stop document in 60 s", file=sys.stderr)
        return 1

    print(f"[py] OK — counts: {seen}")
    # Note: ZMQ PUB/SUB has a slow-joiner property — the first
    # document published *before* the SUB completes its handshake
    # is dropped. Most beamline deployments avoid this by routing
    # through a `bluesky.callbacks.zmq.Proxy` (XSUB-XPUB). Here we
    # only assert events + stop arrived (proves the wire format).
    assert seen["stop"] >= 1, "expected at least one stop doc"
    assert seen["event"] >= 1, "expected at least one event doc"
    return 0


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5577
    sys.exit(main(host, port))
