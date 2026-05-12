from __future__ import annotations

import json
import random
import socket
import struct
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from shared import L


class Operation(StrEnum):
    APPEND = "append"
    REMOVE = "remove"
    GET = "get"
    CONTAINS = "contains"
    POP = "pop"
    PRINT = "print"
    LENGTH = "length"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True)
class Command:
    """
    Represents one client request to the server.

    Examples:
        Command(client_id=1, operation=Operation.APPEND, value=10)
        Command(client_id=2, operation=Operation.GET, index=3)
        Command(client_id=3, operation=Operation.PRINT)
    """

    client_id: int
    operation: Operation
    request_id: int

    # Optional arguments depending on operation
    value: int | None = None
    index: int | None = None

    def validate(self) -> None:
        if self.client_id < 0:
            raise ValueError("client_id must be non-negative")

        if self.request_id < 0:
            raise ValueError("request_id must be non-negative")

        if self.operation in {Operation.APPEND, Operation.REMOVE, Operation.CONTAINS}:
            if self.value is None:
                raise ValueError(f"{self.operation} requires value")

        if self.operation in {Operation.GET, Operation.POP}:
            if self.index is None:
                raise ValueError(f"{self.operation} requires index")

    def to_dict(self) -> dict[str, Any]:
        self.validate()

        return {
            "kind": "command",
            "client_id": self.client_id,
            "request_id": self.request_id,
            "operation": self.operation.value,
            "value": self.value,
            "index": self.index,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Command":
        if data.get("kind") != "command":
            raise ValueError("Message is not a command")

        command = Command(
            client_id=int(data["client_id"]),
            request_id=int(data["request_id"]),
            operation=Operation(data["operation"]),
            value=data.get("value"),
            index=data.get("index"),
        )

        command.validate()
        return command


@dataclass(frozen=True)
class Response:
    """
    Represents one server response.

    result can be:
        - bool
        - int
        - list[int]
        - str
        - None
    """

    request_id: int
    ok: bool
    result: Any = None
    error: str | None = None
    server_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "response",
            "request_id": self.request_id,
            "ok": self.ok,
            "result": self.result,
            "error": self.error,
            "server_id": self.server_id,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Response":
        if data.get("kind") != "response":
            raise ValueError("Message is not a response")

        return Response(
            request_id=int(data["request_id"]),
            ok=bool(data["ok"]),
            result=data.get("result"),
            error=data.get("error"),
            server_id=data.get("server_id"),
        )


_HEADER_SIZE = 4
_MAX_MESSAGE_SIZE = 1_000_000


def _send_json(sock: socket.socket, payload: dict[str, Any]) -> None:
    """
    Sends one JSON message using this format:

        [4-byte unsigned message length][JSON bytes]

    The 4-byte length is stored in network byte order.
    """

    data = json.dumps(payload).encode("utf-8")

    if len(data) > _MAX_MESSAGE_SIZE:
        raise ValueError(f"Message too large: {len(data)} bytes")

    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    """
    Reads exactly n bytes from a socket.

    TCP does not preserve message boundaries, so a single recv()
    is not guaranteed to return the full message.
    """

    chunks: list[bytes] = []
    bytes_remaining = n

    while bytes_remaining > 0:
        chunk = sock.recv(bytes_remaining)

        if not chunk:
            raise ConnectionError("Socket closed while receiving data")

        chunks.append(chunk)
        bytes_remaining -= len(chunk)

    return b"".join(chunks)


def _recv_json(sock: socket.socket) -> dict[str, Any]:
    header = _recv_exactly(sock, _HEADER_SIZE)
    message_size = struct.unpack("!I", header)[0]

    if message_size > _MAX_MESSAGE_SIZE:
        raise ValueError(f"Incoming message too large: {message_size} bytes")

    data = _recv_exactly(sock, message_size)
    return json.loads(data.decode("utf-8"))


def send_command(sock: socket.socket, command: Command) -> None:
    _send_json(sock, command.to_dict())


def recv_command(sock: socket.socket) -> Command:
    return Command.from_dict(_recv_json(sock))


def send_response(sock: socket.socket, response: Response) -> None:
    _send_json(sock, response.to_dict())


def recv_response(sock: socket.socket) -> Response:
    return Response.from_dict(_recv_json(sock))


def send_json_payload(sock: socket.socket, payload: dict[str, Any]) -> None:
    """Length-prefixed JSON for non-command/control peers (e.g. coordinator)."""
    _send_json(sock, payload)


def recv_json_payload(sock: socket.socket) -> dict[str, Any]:
    return _recv_json(sock)


def append(client_id: int, request_id: int, value: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.APPEND,
        value=value,
    )


def command_remove(client_id: int, request_id: int, value: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.REMOVE,
        value=value,
    )


def command_get(client_id: int, request_id: int, index: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.GET,
        index=index,
    )


def command_contains(client_id: int, request_id: int, value: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.CONTAINS,
        value=value,
    )


def command_pop(client_id: int, request_id: int, index: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.POP,
        index=index,
    )


def command_print(client_id: int, request_id: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.PRINT,
    )


def command_length(client_id: int, request_id: int) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.LENGTH,
    )


def command_random(client_id: int, request_id: int) -> Command:
    return random.choice(
        [
            # command_append(client_id, request_id, len(L)),
            command_remove(client_id, request_id, random.choice(L)),
            command_get(client_id, request_id, random.randint(0, len(L) - 1)),
            command_contains(client_id, request_id, random.choice(L)),
            command_pop(client_id, request_id, random.randint(0, len(L) - 1)),
            command_print(client_id, request_id),
        ]
    )


def command_shutdown(client_id: int = 0, request_id: int = 0) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.SHUTDOWN,
    )
