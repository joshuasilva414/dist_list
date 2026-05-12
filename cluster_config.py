from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServerClusterConfig:
    coordinator_host: str
    coordinator_port: int
    expected_servers: int
    advertise_host: str = "127.0.0.1"
