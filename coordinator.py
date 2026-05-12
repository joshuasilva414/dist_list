from __future__ import annotations

import json
import socket
import threading
import time
from multiprocessing import Queue
from typing import Any

from cluster_config import ServerClusterConfig
from protocol import recv_json_payload, send_json_payload

KIND_REGISTER = "coord_register"
KIND_REGISTER_REPLY = "coord_register_reply"
KIND_GET_MEMBERS = "coord_get_members"
KIND_GET_MEMBERS_REPLY = "coord_get_members_reply"


def _members_snapshot(members: dict[int, tuple[str, int]]) -> dict[str, dict[str, Any]]:
    return {
        str(sid): {"host": host, "port": port}
        for sid, (host, port) in sorted(members.items())
    }


def _parse_members(data: dict[str, Any]) -> dict[int, tuple[str, int]]:
    raw = data.get("members") or {}
    out: dict[int, tuple[str, int]] = {}
    for k, v in raw.items():
        sid = int(k)
        out[sid] = (str(v["host"]), int(v["port"]))
    return out


def run_coordinator(
    expected_servers: int,
    bind_host: str,
    port_queue: Queue,
    debug: bool = False,
) -> None:
    def log(msg: str) -> None:
        if debug:
            print(f"[coordinator]: {msg}")

    members_lock = threading.Lock()
    members: dict[int, tuple[str, int]] = {}

    def handle_connection(conn: socket.socket, addr: tuple[str, int]) -> None:
        nonlocal members
        with conn:
            log(f"connection from {addr}")
            try:
                msg = recv_json_payload(conn)
            except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as e:
                log(f"bad message from {addr}: {e}")
                return

            kind = msg.get("kind")
            with members_lock:
                if kind == KIND_REGISTER:
                    sid = int(msg["server_id"])
                    host = str(msg["host"])
                    port = int(msg["port"])
                    members[sid] = (host, port)
                    ready = len(members) >= expected_servers
                    reply = {
                        "kind": KIND_REGISTER_REPLY,
                        "members": _members_snapshot(members),
                        "ready": ready,
                    }
                elif kind == KIND_GET_MEMBERS:
                    ready = len(members) >= expected_servers
                    reply = {
                        "kind": KIND_GET_MEMBERS_REPLY,
                        "members": _members_snapshot(members),
                        "ready": ready,
                    }
                else:
                    reply = {
                        "kind": "coord_error",
                        "error": f"unknown kind: {kind!r}",
                    }
            try:
                send_json_payload(conn, reply)
            except OSError as e:
                log(f"send failed to {addr}: {e}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((bind_host, 0))
        sock.listen()
        host, port = sock.getsockname()
        port_queue.put((host, port))
        log(f"listening on {host}:{port}, expecting {expected_servers} servers")

        sock.settimeout(0.5)
        while True:
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            t = threading.Thread(
                target=handle_connection, args=(conn, addr), daemon=True
            )
            t.start()


def fetch_cluster_members(
    cluster: ServerClusterConfig,
) -> tuple[dict[int, tuple[str, int]], bool]:
    with socket.create_connection(
        (cluster.coordinator_host, cluster.coordinator_port)
    ) as s:
        send_json_payload(s, {"kind": KIND_GET_MEMBERS})
        reply = recv_json_payload(s)
    if reply.get("kind") != KIND_GET_MEMBERS_REPLY:
        raise RuntimeError(f"coordinator error: {reply}")
    return _parse_members(reply), bool(reply.get("ready"))


def register_with_coordinator(
    cluster: ServerClusterConfig,
    server_id: int,
    listen_port: int,
) -> tuple[dict[int, tuple[str, int]], bool]:
    with socket.create_connection(
        (cluster.coordinator_host, cluster.coordinator_port)
    ) as s:
        send_json_payload(
            s,
            {
                "kind": KIND_REGISTER,
                "server_id": server_id,
                "host": cluster.advertise_host,
                "port": listen_port,
            },
        )
        reply = recv_json_payload(s)
    if reply.get("kind") != KIND_REGISTER_REPLY:
        raise RuntimeError(f"coordinator error: {reply}")
    return _parse_members(reply), bool(reply.get("ready"))


def wait_for_peer_map(
    cluster: ServerClusterConfig,
    server_id: int,
    listen_port: int,
    poll_interval_s: float = 0.05,
) -> dict[int, tuple[str, int]]:
    while True:
        peers, ready = register_with_coordinator(cluster, server_id, listen_port)
        if ready and len(peers) >= cluster.expected_servers:
            return peers
        time.sleep(poll_interval_s)
