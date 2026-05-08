from multiprocessing import Process
from socket import socket
from client import client
from protocol import send_command, command_shutdown
from server import server
import argparse
from utils import find_free_port
import random

NUM_SERVERS = 3
NUM_CLIENTS = 3
SERVER_BASE_PORT = 10000
OPERATIONS = 50
DEBUG = True


def main():
    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-servers", type=int, default=NUM_SERVERS)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--server-base-port", type=int, default=SERVER_BASE_PORT)
    parser.add_argument("--operations", type=int, default=OPERATIONS)
    parser.add_argument("--debug", action="store_true", default=False)
    args = parser.parse_args()
    num_servers = args.num_servers
    num_clients = args.num_clients
    server_base_port = args.server_base_port
    operations = args.operations
    debug = args.debug

    # start servers
    next_server_port = find_free_port(server_base_port)
    servers: list[tuple[int, Process]] = []
    for i in range(num_servers):
        s = Process(target=server, args=(i, next_server_port, debug))
        servers.append((next_server_port, s))
        s.start()
        next_server_port = find_free_port(next_server_port + 1)

    clients: list[Process] = []
    for i in range(num_clients):
        target_server_port = random.choice(servers)[0]
        c = Process(target=client, args=(i, target_server_port, operations, debug))
        clients.append(c)
        c.start()

    for c in clients:
        c.join()

    for s in servers:
        port, server_process = s
        with socket.create_connection(("localhost", port)) as conn:
            send_command(conn, command_shutdown())
        server_process.join()


if __name__ == "__main__":
    main()
