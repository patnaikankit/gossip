# gossip

## Summary

This is a small Java 21 UDP gossip demonstration. Each process binds a UDP port, sends JSON heartbeats to peers, learns addresses from peers’ membership lists, and redraws a terminal table showing each peer as alive or dead. It is suitable for illustrating the shape of peer discovery on one machine. The current implementation does not reliably establish a convergent, accurate cluster membership view, and it is explicitly unsuitable for production or untrusted networks.

## Requirements

- Java 21 or newer
- Maven

## Build

```bash
mvn -DskipTests clean compile
```

## Run a local cluster

Start a seed node in one terminal:

```bash
mvn -Dport=8000 -DnodeId=node-a exec:java
```

Start a second node in another terminal, using the first as a seed:

```bash
mvn -Dport=8001 -DnodeId=node-b -Dseeds=127.0.0.1:8000 exec:java
```

Start additional nodes with a unique port and one or more comma-separated seed endpoints:

```bash
mvn -Dport=8002 -DnodeId=node-c -Dseeds=127.0.0.1:8001 exec:java
```

Each node displays discovered peers and their current liveness state. A relayed peer starts as `DISCOVERED`; it becomes `ALIVE` after a direct heartbeat. A missed peer progresses through `SUSPECT` to `DEAD`.

## Configuration

| System property | Default | Description |
|---|---|---|
| `port` | `8000` | Local UDP port to bind. |
| `nodeId` | Random UUID | Node identity. Set explicitly to keep the same identity across restarts. |
| `host` | `127.0.0.1` | Address advertised to other nodes. Set this to a reachable address for multi-host use. |
| `seeds` | Empty | Comma-separated seed endpoints in `host:port` format. Bracket IPv6 addresses, for example `[::1]:8000`. |

## Protocol notes

- Heartbeats use JSON over UDP.
- Each round sends to at most three randomly selected known peers.
- Heartbeats include a bounded sample of membership information and are limited to 1,200 bytes.
- The peer table is capped at 500 entries and expires entries that have not been heard about for 60 seconds.
- Liveness uses local monotonic time. Timestamps are not shared between nodes.
