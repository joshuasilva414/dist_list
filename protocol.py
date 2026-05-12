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
    INSERT = "insert"
    REPLACE = "replace"
    REMOVE = "remove"
    GET = "get"
    CONTAINS = "contains"
    POP = "pop"
    PRINT = "print"
    LENGTH = "length"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True)
class Command:
    client_id: int
    operation: Operation
    request_id: int

    value: str | None = None
    index: int | None = None

    def validate(self) -> None:
        if self.client_id < 0:
            raise ValueError("client_id must be non-negative")

        if self.request_id < 0:
            raise ValueError("request_id must be non-negative")

        if self.operation in {
            Operation.APPEND,
            Operation.REMOVE,
            Operation.CONTAINS,
        }:
            if self.value is None:
                raise ValueError(f"{self.operation} requires value")

        if self.operation in {Operation.INSERT, Operation.REPLACE}:
            if self.index is None:
                raise ValueError(f"{self.operation} requires index")
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

        raw_val = data.get("value")
        val = None if raw_val is None else str(raw_val)
        raw_idx = data.get("index")
        idx = None if raw_idx is None else int(raw_idx)

        command = Command(
            client_id=int(data["client_id"]),
            request_id=int(data["request_id"]),
            operation=Operation(data["operation"]),
            value=val,
            index=idx,
        )

        command.validate()
        return command


@dataclass(frozen=True)
class Response:
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
    data = json.dumps(payload).encode("utf-8")

    if len(data) > _MAX_MESSAGE_SIZE:
        raise ValueError(f"Message too large: {len(data)} bytes")

    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
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
    _send_json(sock, payload)


def recv_json_payload(sock: socket.socket) -> dict[str, Any]:
    return _recv_json(sock)


def recv_json_payload_or_none(sock: socket.socket) -> dict[str, Any] | None:
    first = sock.recv(_HEADER_SIZE)
    if not first:
        return None
    if len(first) < _HEADER_SIZE:
        first += _recv_exactly(sock, _HEADER_SIZE - len(first))
    message_size = struct.unpack("!I", first)[0]

    if message_size > _MAX_MESSAGE_SIZE:
        raise ValueError(f"Incoming message too large: {message_size} bytes")

    data = _recv_exactly(sock, message_size)
    return json.loads(data.decode("utf-8"))


def append(client_id: int, request_id: int, value: str) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.APPEND,
        value=value,
    )


def command_insert(client_id: int, request_id: int, index: int, value: str) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.INSERT,
        index=index,
        value=value,
    )


def command_replace(client_id: int, request_id: int, index: int, value: str) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.REPLACE,
        index=index,
        value=value,
    )


def command_remove(client_id: int, request_id: int, value: str) -> Command:
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


def command_contains(client_id: int, request_id: int, value: str) -> Command:
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
    """Weighted mix: 40% insert, 30% delete (pop), 20% replace, 10% append."""
    pool = L
    span = max(len(pool) * 4, 8)
    kind = random.choices(
        ("insert", "delete", "replace", "append"),
        weights=[40, 30, 20, 10],
        k=1,
    )[0]
    word = random.choice(pool)
    if kind == "insert":
        idx = random.randint(0, span)
        return command_insert(client_id, request_id, idx, word)
    if kind == "delete":
        idx = random.randint(0, span - 1)
        return command_pop(client_id, request_id, idx)
    if kind == "replace":
        idx = random.randint(0, span - 1)
        return command_replace(client_id, request_id, idx, word)
    return append(client_id, request_id, word)


def command_shutdown(client_id: int = 0, request_id: int = 0) -> Command:
    return Command(
        client_id=client_id,
        request_id=request_id,
        operation=Operation.SHUTDOWN,
    )
