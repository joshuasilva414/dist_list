import socket
from shared import L
from threading import Event, Lock, Thread
from protocol import recv_command, send_response, Response, Operation, Command

shared_list = L.copy()
lock = Lock()


def handle_command(id: int, command: Command) -> Response:
    with lock:
        if command.operation == Operation.APPEND:
            shared_list.append(command.value)

            return Response(command.request_id, True, result=True, server_id=id)

        if command.operation == Operation.GET:
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


def handle_connection(
    server_id: int,
    conn: socket.socket,
    addr: tuple[str, int],
    shutdown_event: Event,
    debug: bool = False,
):
    def log(message: str) -> None:
        if debug:
            print(f"[server {server_id}]: {message}")

    with conn:
        log(f"connection made from {addr}")
        while True:
            try:
                command = recv_command(conn)
            except ConnectionError:
                break
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
            response = handle_command(server_id, command)
            send_response(conn, response)


def server(id, port, debug=False):
    def log(message):
        if debug:
            print(f"[server {id}]: {message}")

    log(f"starting on port {port}")
    # start server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("localhost", port))
        s.listen()
        log(f"listening on localhost:{port}")
        shutdown_event = Event()
        threads: list[Thread] = []
        s.settimeout(0.5)

        while not shutdown_event.is_set():
            try:
                conn, addr = s.accept()
            except socket.timeout:
                continue

            t = Thread(
                target=handle_connection,
                args=(id, conn, addr, shutdown_event, debug),
                daemon=True,
            )
            threads.append(t)
            t.start()

        log("shutting down")
        for t in threads:
            t.join(timeout=1.0)
