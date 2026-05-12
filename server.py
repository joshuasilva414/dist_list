import socket
import time
from threading import Event, Lock, Thread

from cluster_config import ServerClusterConfig
from coordinator import wait_for_peer_map
from operations_queue import (
    KIND_REPLICA_HELLO,
    OperationsQueue,
)
from protocol import (
    Command,
    Operation,
    Response,
    recv_command,
    recv_json_payload,
    recv_json_payload_or_none,
    send_json_payload,
    send_response,
)
from shared import L

shared_list = L.copy()
lock = Lock()


def handle_command(id: int, command: Command) -> Response:
    with lock:
        if command.operation == Operation.GET:
            if command.index is None or not (0 <= command.index < len(shared_list)):
                return Response(
                    command.request_id,
                    True,
                    result=None,
                    server_id=id,
                )
            return Response(
                command.request_id,
                True,
                result=shared_list[command.index],
                server_id=id,
            )

        if command.operation == Operation.CONTAINS:
            return Response(
                command.request_id,
                True,
                result=command.value in shared_list,
                server_id=id,
            )

        if command.operation == Operation.PRINT:
            return Response(
                command.request_id, True, result=list(shared_list), server_id=id
            )

    return Response(
        request_id=command.request_id,
        ok=False,
        error=f"unsupported operation: {command.operation}",
        server_id=id,
    )


def _peer_reader_loop(
    server_id: int,
    opq: OperationsQueue,
    peer_id: int,
    conn: socket.socket,
    shutdown_event: Event,
    debug: bool,
):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id} peer {peer_id}]: {message}")

    try:
        while not shutdown_event.is_set():
            try:
                msg = recv_json_payload_or_none(conn)
            except (ConnectionError, OSError, ValueError) as e:
                log(f"peer link error: {e}")
                break
            if msg is None:
                break
            kind = msg.get("kind")
            if kind == "replica_update":
                opq.on_replica_update(msg)
            elif kind == "replica_ack":
                opq.on_replica_ack(msg)
            else:
                log(f"unexpected replica message {kind!r}")
    finally:
        opq.unregister_peer_socket(peer_id)
        try:
            conn.close()
        except OSError:
            pass


def _mesh_connector(
    server_id: int,
    peers: dict[int, tuple[str, int]],
    opq: OperationsQueue,
    shutdown_event: Event,
    debug: bool,
):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id} mesh]: {message}")

    for peer_id, (host, port) in sorted(peers.items()):
        if peer_id <= server_id:
            continue
        while not shutdown_event.is_set():
            try:
                conn = socket.create_connection((host, port), timeout=2.0)
            except OSError as e:
                log(f"connect to {peer_id} at {host}:{port} failed ({e}), retrying")
                time.sleep(0.05)
                continue
            conn.settimeout(None)
            try:
                send_json_payload(
                    conn,
                    {"kind": KIND_REPLICA_HELLO, "server_id": server_id},
                )
            except OSError as e:
                log(f"hello to {peer_id} failed: {e}")
                conn.close()
                time.sleep(0.05)
                continue
            opq.register_peer_socket(peer_id, conn)
            Thread(
                target=_peer_reader_loop,
                args=(server_id, opq, peer_id, conn, shutdown_event, debug),
                daemon=True,
            ).start()
            log(f"outbound link ready to {peer_id}")
            break


def _handle_client_session(
    server_id: int,
    conn: socket.socket,
    addr: tuple[str, int],
    shutdown_event: Event,
    opq: OperationsQueue,
    first_command: Command,
    debug: bool = False,
):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id}]: {message}")

    with conn:
        log(f"client session from {addr}")
        command = first_command
        while True:
            try:
                if command.operation == Operation.SHUTDOWN:
                    shutdown_event.set()
                    send_response(
                        conn,
                        Response(
                            request_id=command.request_id,
                            ok=True,
                            result=True,
                            server_id=server_id,
                        ),
                    )
                    break
                if opq.is_mutation(command.operation):
                    response = opq.submit_local_mutation(command)
                else:
                    response = handle_command(server_id, command)
                send_response(conn, response)
                command = recv_command(conn)
            except ConnectionError:
                break


def _dispatch_incoming(
    server_id: int,
    conn: socket.socket,
    addr: tuple[str, int],
    shutdown_event: Event,
    opq: OperationsQueue,
    debug: bool,
):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id}]: {message}")

    try:
        first = recv_json_payload(conn)
    except (ConnectionError, OSError, ValueError) as e:
        log(f"first message failed from {addr}: {e}")
        conn.close()
        return

    kind = first.get("kind")
    if kind == KIND_REPLICA_HELLO:
        peer_id = int(first["server_id"])
        opq.register_peer_socket(peer_id, conn)
        Thread(
            target=_peer_reader_loop,
            args=(server_id, opq, peer_id, conn, shutdown_event, debug),
            daemon=True,
        ).start()
        if debug:
            print(f"[server {server_id}]: inbound replica {peer_id} from {addr}")
        return

    if kind == "command":
        cmd = Command.from_dict(first)
        _handle_client_session(server_id, conn, addr, shutdown_event, opq, cmd, debug)
        return

    log(f"unknown first message kind {kind!r} from {addr}")
    conn.close()


def server(server_id: int, cluster: ServerClusterConfig, debug: bool = False):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id}]: {message}")

    opq = OperationsQueue(
        server_id,
        cluster.expected_servers,
        shared_list,
        lock,
        debug=debug,
    )
    opq.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", 0))
        listen_port = int(s.getsockname()[1])
        s.listen()
        log(
            f"listening on 0.0.0.0:{listen_port} (advertise {cluster.advertise_host}:{listen_port} to peers)"
        )
        peers = wait_for_peer_map(cluster, server_id, listen_port)
        log(f"cluster membership ready: {peers}")

        others = {k: v for k, v in peers.items() if k != server_id}
        shutdown_event = Event()
        threads: list[Thread] = []

        Thread(
            target=_mesh_connector,
            args=(server_id, others, opq, shutdown_event, debug),
            daemon=True,
        ).start()

        need_peer_links = len(others)
        mesh_logged = False
        s.settimeout(0.05)

        while not shutdown_event.is_set():
            if (
                not mesh_logged
                and need_peer_links > 0
                and opq.peer_link_count() >= need_peer_links
            ):
                log(f"replica mesh ready ({opq.peer_link_count()} peer links)")
                mesh_logged = True
            if need_peer_links == 0 and not mesh_logged:
                log("replica mesh ready (standalone)")
                mesh_logged = True

            try:
                conn, addr = s.accept()
            except socket.timeout:
                continue

            t = Thread(
                target=_dispatch_incoming,
                args=(server_id, conn, addr, shutdown_event, opq, debug),
                daemon=True,
            )
            threads.append(t)
            t.start()

        log("shutting down")
        opq.close_peer_links()
        opq.stop()
        for t in threads:
            t.join(timeout=1.0)
        opq.join_delivery()
        with lock:
            final = list(shared_list)
        print(
            f"[server {server_id}] final list ({len(final)} elements): {final}",
            flush=True,
        )
