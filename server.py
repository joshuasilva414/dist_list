import socket
from shared import L
from threading import Lock, Thread
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


def handle_connection(conn: socket.socket, addr: tuple[str, int]):
    with conn:
        while True:
            command = recv_command(conn)
            if command.operation == Operation.SHUTDOWN:
                break
            else:
                with lock:
                    shared_list.append(command.value)
                    send_response(
                        conn,
                        Response(ok=True, request_id=command.request_id, server_id=id),
                    )


def server(id, port, debug=False):
    def log(message):
        if debug:
            print(f"[server {id}]: {message}")

    log(f"starting on port {port}")
    # start server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", port))
        s.listen()
        log(f"listening on localhost:{port}")
        while True:
            conn, addr = s.accept()
            t = Thread(target=handle_connection, args=(conn, addr))
            t.start()
            log(f"connection made from {addr}")
            command = recv_command(conn)
            if command.operation == Operation.SHUTDOWN:
                log("shutting down")
                break
            else:
                log(f"received command: {command}")
                send_response(
                    conn, Response(ok=True, request_id=command.request_id, server_id=id)
                )
