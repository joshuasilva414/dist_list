import socket


def find_free_port(start: int) -> int:
    def is_port_in_use(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("localhost", port)) == 0

    port = start
    while is_port_in_use(port):
        port += 1
    return port
