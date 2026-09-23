"""Serve a blop-protocol optimizer to bsrs over ZMQ REQ/REP.

bsrs's ``blop::bridge::BridgeOptimizer`` (feature ``blop-bridge``) is the
client: it sends one JSON request per REQ frame and reads one JSON reply.
The optimizer behind the server is any object with blop's ``Optimizer``
surface (``suggest`` / ``ingest`` / ``get_best_points``), plus the optional
``register_suggestions``, ``register_failures``, ``should_stop`` and
``checkpoint`` methods, which the server reports as capabilities.
"""

from .server import Backend, Server, serve

__all__ = ["Backend", "Server", "serve"]
