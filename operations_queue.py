"""
Lamport totally-ordered multicast queue with full acknowledgements.

Each server owns one ``OperationsQueue`` instance that holds:

- the local Lamport clock and a monotonic ``local_seq`` for outbound updates
- a priority queue (min-heap) of pending updates keyed by ``(ts, sender_id, local_seq)``
- a per-message ACK set tracking which servers have acknowledged the message
- the outbound peer sockets (one per peer, with a send lock)
- a dedicated delivery thread that pops the head of the heap once *every* server
  has acknowledged it and applies the mutation under the shared list lock

Wire kinds:
    replica_hello   -- one-time handshake on each peer connection
    replica_update  -- carries a multicast mutation with its Lamport timestamp
    replica_ack     -- acknowledges a specific (msg_sender_id, msg_local_seq)
"""

from __future__ import annotations

import errno
import heapq
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from protocol import Command, Operation, Response, send_json_payload

KIND_REPLICA_HELLO = "replica_hello"
KIND_REPLICA_UPDATE = "replica_update"
KIND_REPLICA_ACK = "replica_ack"

_MUTATION_OPS: frozenset[Operation] = frozenset(
    {Operation.APPEND, Operation.REMOVE, Operation.POP}
)


@dataclass(order=True)
class _Pending:
    """One entry in the per-server priority queue.

    ``sort_key`` provides the strict total order ``(ts, sender_id, local_seq)``
    (Lamport timestamp with tie-break on the originating server id, then on the
    sender's monotonic local sequence). Only ``sort_key`` participates in
    comparisons; everything else is metadata.
    """

    sort_key: tuple[int, int, int]
    sender_id: int = field(compare=False)
    local_seq: int = field(compare=False)
    ts: int = field(compare=False)
    command: Command = field(compare=False)
    # Set only for locally-originated mutations; the client-facing thread waits on it.
    delivered: threading.Event | None = field(default=None, compare=False)
    # Populated by the delivery loop right before ``delivered`` is set.
    result: Response | None = field(default=None, compare=False)


class OperationsQueue:
    def __init__(
        self,
        server_id: int,
        expected_servers: int,
        shared_list: list[Any],
        lock: threading.Lock,
        debug: bool = False,
    ) -> None:
        self.server_id = server_id
        self.expected_servers = expected_servers
        self.shared_list = shared_list
        # The same lock guards ``shared_list`` and the queue state, so a read
        # serialized through ``handle_command`` always sees a consistent state
        # with respect to delivery.
        self.lock = lock
        self.debug = debug

        self._cv = threading.Condition(lock)
        self._lamport = 0
        self._local_seq = 0
        self._heap: list[_Pending] = []
        self._acks: dict[tuple[int, int], set[int]] = {}

        # Outbound peer sockets and per-socket send locks. Kept on a separate
        # lock so socket I/O never serializes against the queue critical section.
        self._peer_lock = threading.Lock()
        self._peer_sockets: dict[int, socket.socket] = {}
        self._peer_send_locks: dict[int, threading.Lock] = {}

        self._delivery_thread: threading.Thread | None = None
        self._running = False

    def _log(self, msg: str) -> None:
        if self.debug:
            print(f"[server {self.server_id} opq]: {msg}")

    @staticmethod
    def is_mutation(op: Operation) -> bool:
        return op in _MUTATION_OPS

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._delivery_thread = threading.Thread(
            target=self._delivery_loop,
            name=f"opq-deliver-{self.server_id}",
            daemon=True,
        )
        self._delivery_thread.start()

    def stop(self) -> None:
        with self._cv:
            self._running = False
            self._cv.notify_all()

    def peer_link_count(self) -> int:
        with self._peer_lock:
            return len(self._peer_sockets)

    def register_peer_socket(self, peer_id: int, conn: socket.socket) -> None:
        with self._peer_lock:
            if peer_id in self._peer_sockets:
                self._log(f"peer {peer_id} already registered, ignoring duplicate")
                return
            self._peer_sockets[peer_id] = conn
            self._peer_send_locks[peer_id] = threading.Lock()
        self._log(f"registered peer {peer_id}")

    def unregister_peer_socket(self, peer_id: int) -> None:
        with self._peer_lock:
            self._peer_sockets.pop(peer_id, None)
            self._peer_send_locks.pop(peer_id, None)
        self._log(f"unregistered peer {peer_id}")

    def close_peer_links(self) -> None:
        """Half-close the write side of every registered peer socket so each
        peer's reader sees a clean FIN at its next ``recv``. This lets the
        cluster wind down without anyone going through the
        ``ConnectionError`` / "Socket closed while receiving data" path."""
        with self._peer_lock:
            targets = list(self._peer_sockets.items())
        for peer_id, conn in targets:
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError as e:
                # Another thread may have closed the fd right after our snapshot,
                # or shutdown may be redundant once the endpoint is disconnected.
                if e.errno in {
                    errno.EBADF,
                    errno.ENOTCONN,
                    errno.EINVAL,
                    errno.ECONNRESET,
                }:
                    continue
                self._log(f"shutdown(SHUT_WR) on peer {peer_id} failed: {e}")

    def _multicast(self, payload: dict[str, Any]) -> None:
        """Send ``payload`` to every peer we currently have a link to."""
        with self._peer_lock:
            targets = [
                (pid, conn, self._peer_send_locks[pid])
                for pid, conn in self._peer_sockets.items()
            ]
        for peer_id, conn, send_lock in targets:
            with send_lock:
                try:
                    send_json_payload(conn, payload)
                except OSError as e:
                    self._log(f"send to peer {peer_id} failed: {e}")

    def _wait_mesh_ready(self) -> None:
        """Block until every other replica has connected to us.

        Multicasting before the mesh is fully formed would silently drop
        updates to the missing peers and we would never collect their ACKs.
        """
        needed = max(0, self.expected_servers - 1)
        while self.peer_link_count() < needed:
            time.sleep(0.01)

    def submit_local_mutation(self, command: Command) -> Response:
        """Multicast ``command`` to all peers, self-deliver, and block until
        the delivery thread applies it locally.

        Returns the ``Response`` captured at the moment of application so the
        client sees a value consistent with every other replica.
        """
        self._wait_mesh_ready()

        delivered = threading.Event()
        with self._cv:
            # Lamport: local event before send.
            self._lamport += 1
            ts = self._lamport
            self._local_seq += 1
            local_seq = self._local_seq
            pending = _Pending(
                sort_key=(ts, self.server_id, local_seq),
                sender_id=self.server_id,
                local_seq=local_seq,
                ts=ts,
                command=command,
                delivered=delivered,
            )
            heapq.heappush(self._heap, pending)
            msg_id = (self.server_id, local_seq)
            # Sender's own ack is implicit on submit; the delivery rule expects
            # every server (including us) to have acknowledged the message.
            self._acks.setdefault(msg_id, set()).add(self.server_id)
            self._cv.notify_all()

        update = {
            "kind": KIND_REPLICA_UPDATE,
            "sender_id": self.server_id,
            "local_seq": local_seq,
            "ts": ts,
            "command": command.to_dict(),
        }
        self._multicast(update)

        delivered.wait()
        # ``result`` was written by the delivery thread before ``set()``;
        # Event.set/wait provides the necessary happens-before barrier.
        assert pending.result is not None
        return pending.result

    def on_replica_update(self, msg: dict[str, Any]) -> None:
        """Handle a ``replica_update`` from a peer: enqueue it in timestamp
        order, record the originator's implicit ACK plus our own, and multicast
        our explicit ACK to every other peer."""
        sender_id = int(msg["sender_id"])
        local_seq = int(msg["local_seq"])
        ts = int(msg["ts"])
        command = Command.from_dict(msg["command"])

        with self._cv:
            # Lamport: max(local, incoming) + 1 on receive.
            self._lamport = max(self._lamport, ts) + 1
            ack_ts = self._lamport
            pending = _Pending(
                sort_key=(ts, sender_id, local_seq),
                sender_id=sender_id,
                local_seq=local_seq,
                ts=ts,
                command=command,
            )
            heapq.heappush(self._heap, pending)
            msg_id = (sender_id, local_seq)
            acks = self._acks.setdefault(msg_id, set())
            acks.add(sender_id)
            acks.add(self.server_id)
            self._cv.notify_all()

        ack = {
            "kind": KIND_REPLICA_ACK,
            "sender_id": self.server_id,
            "msg_sender_id": sender_id,
            "msg_local_seq": local_seq,
            "ts": ack_ts,
        }
        self._multicast(ack)

    def on_replica_ack(self, msg: dict[str, Any]) -> None:
        ack_from = int(msg["sender_id"])
        msg_sender = int(msg["msg_sender_id"])
        msg_local_seq = int(msg["msg_local_seq"])
        ack_ts = int(msg["ts"])

        with self._cv:
            self._lamport = max(self._lamport, ack_ts) + 1
            self._acks.setdefault((msg_sender, msg_local_seq), set()).add(ack_from)
            self._cv.notify_all()

    def _delivery_loop(self) -> None:
        """Single delivery thread: pop the head whenever every server has
        acknowledged it and apply the mutation to ``shared_list``."""
        with self._cv:
            while self._running:
                while self._heap:
                    head = self._heap[0]
                    msg_id = (head.sender_id, head.local_seq)
                    if len(self._acks.get(msg_id, set())) < self.expected_servers:
                        break
                    heapq.heappop(self._heap)
                    self._acks.pop(msg_id, None)
                    response = self._apply(head.command)
                    head.result = response
                    if head.delivered is not None:
                        head.delivered.set()
                    self._log(
                        f"delivered ts={head.ts} from={head.sender_id} "
                        f"seq={head.local_seq} op={head.command.operation}"
                    )
                if not self._running:
                    return
                self._cv.wait(timeout=0.5)

    def _apply(self, command: Command) -> Response:
        """Apply a mutation to ``shared_list`` deterministically and return the
        ``Response`` that the originating server should hand back to its client.
        Every replica runs this on the same command in the same order, so the
        results are identical across the cluster."""
        op = command.operation
        try:
            if op == Operation.APPEND:
                self.shared_list.append(command.value)
                result: Any = True
            elif op == Operation.REMOVE:
                try:
                    self.shared_list.remove(command.value)
                    result = True
                except ValueError:
                    result = False
            elif op == Operation.POP:
                if command.index is None or not (
                    0 <= command.index < len(self.shared_list)
                ):
                    result = None
                else:
                    result = self.shared_list.pop(command.index)
            else:
                return Response(
                    request_id=command.request_id,
                    ok=False,
                    error=f"non-mutation passed to ordered apply: {op}",
                    server_id=self.server_id,
                )
        except Exception as e:
            return Response(
                request_id=command.request_id,
                ok=False,
                error=f"{type(e).__name__}: {e}",
                server_id=self.server_id,
            )
        return Response(
            request_id=command.request_id,
            ok=True,
            result=result,
            server_id=self.server_id,
        )
