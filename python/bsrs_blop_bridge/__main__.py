"""``python -m bsrs_blop_bridge --endpoint tcp://127.0.0.1:5590 --backend ax``."""

from __future__ import annotations

import argparse
import logging
import sys

from .backends import BACKENDS
from .server import serve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve a blop optimizer to bsrs over ZMQ REQ/REP.")
    parser.add_argument("--endpoint", default="tcp://127.0.0.1:5590", help="ZMQ bind endpoint; a wildcard port (`:*`) is resolved and printed")
    parser.add_argument("--backend", choices=sorted(BACKENDS), default="ax")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr)

    def ready(bound: str) -> None:
        # One line a parent process can read to learn the resolved endpoint.
        print(f"endpoint {bound}", flush=True)

    try:
        serve(args.endpoint, BACKENDS[args.backend], ready=ready)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
