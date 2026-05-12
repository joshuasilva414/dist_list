# Distributed List Project for CS 5523: Operating Systems

## Program status: Fully Completed

## Synchronization and Ordering

- The coordinator serializes reads and writes of the membership map using a threading lock while each connection is handled on its own thread.
- On every server, the shared list and mutation path are guarded: local reads take the shared list lock, while OperationsQueue uses that same lock with a condition variable plus a Lamport-style logical clock so replica updates stay totally ordered across the cluster.
- A mutation is multicast to peers, servers exchange acknowledgements, and the delivery loop applies each operation only after every server has acked—that quorum plus the heap ordering keeps servers consistent despite concurrent clients and overlapping network traffic.

## How to Run

```bash
python3 main.py
```

Optional flags: `--num-servers`, `--num-clients`, `--operations`(number of operations), and `--debug` (see server output).

### One liner for meeting requirements:

```bash
python3 main.py --num-servers 3 --num-clients 3 --operations 50 --debug
```

## Help

AI helped troubleshoot synchronization issues and developing a plan for implementation.

## Assignment Feedback

- Very helpful to learn about distributed systems and synchronization.
