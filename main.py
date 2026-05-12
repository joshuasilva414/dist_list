import argparse
import queue
import random
import socket
import time
from multiprocessing import Process, Queue

from cluster_config import ServerClusterConfig
from client import client
from coordinator import fetch_cluster_members, run_coordinator
from protocol import (
    command_shutdown,
    send_command,
)
from server import server

NUM_SERVERS = 3
NUM_CLIENTS = 3
OPERATIONS = 50


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-servers", type=int, default=NUM_SERVERS)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--operations", type=int, default=OPERATIONS)
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    num_servers = args.num_servers
    num_clients = args.num_clients
    operations = args.operations
    debug = args.debug

    coord_host = "127.0.0.1"
    coord_q: Queue = Queue()
    coord_proc = Process(
        target=run_coordinator,
        args=(num_servers, coord_host, coord_q, debug),
        daemon=True,
    )
    coord_proc.start()
    try:
        coord_host_reported, coord_port = coord_q.get(timeout=30)
    except queue.Empty as e:
        raise RuntimeError("coordinator did not publish its listen address") from e

    cluster = ServerClusterConfig(
        coordinator_host=coord_host_reported,
        coordinator_port=coord_port,
        expected_servers=num_servers,
        advertise_host=coord_host,
    )
    servers: list[Process] = []
    for i in range(num_servers):
        p = Process(target=server, args=(i, cluster, debug))
        servers.append(p)
        p.start()

    members: dict[int, tuple[str, int]] | None = None
    for _ in range(600):
        m, ready = fetch_cluster_members(cluster)
        if ready and len(m) >= num_servers:
            members = m
            break
        time.sleep(0.05)
    if members is None:
        raise RuntimeError("cluster membership did not become ready in time")

    clients: list[Process] = []
    for i in range(num_clients):
        _host, target_port = random.choice(list(members.values()))
        c = Process(target=client, args=(i, target_port, operations, debug))
        clients.append(c)
        c.start()

    for c in clients:
        c.join()

    for sid in sorted(members.keys()):
        host, port = members[sid]
        with socket.create_connection((host, port)) as conn:
            send_command(conn, command_shutdown())

    for p in servers:
        p.join()


if __name__ == "__main__":
    main()
