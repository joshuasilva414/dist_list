import socket

from protocol import recv_response, send_command, command_random


def client(id, port, operations=50, debug=False):
    def log(message):
        if debug:
            print(f"[client {id}]: {message}")

    log(f"connecting to server on port {port} with {operations} operations")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", port))
        log(f"connected to server on port {port}")
        for i in range(operations):
            command = command_random(id, i)
            log(f"sending command: {command}")
            send_command(s, command)
            response = recv_response(s)
            log(f"received response: {response}")
