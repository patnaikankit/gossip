package com.patnaikankit.gossip;

import com.google.gson.Gson;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

final class GossipNode {
    private static final Logger logger = Logger.getLogger(GossipNode.class.getName());
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(500);
    private static final Duration BASE_SUSPECT_AFTER = Duration.ofMillis(2500);
    private static final Duration BASE_DEAD_AFTER = Duration.ofSeconds(5);
    private static final Duration MEMBERSHIP_RETENTION = Duration.ofSeconds(60);
    private static final int MAX_DATAGRAM_BYTES = 1200;
    private static final int MAX_GOSSIP_PEERS = 16;
    private static final int MAX_KNOWN_PEERS = 500;
    private static final int FANOUT = 3;

    private final Gson gson = new Gson();
    private final PeerInfo self;
    private final String incarnationId = java.util.UUID.randomUUID().toString();
    private final List<Endpoint> seeds;
    private final ConcurrentHashMap<String, PeerState> peers = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final DatagramSocket socket;
    private volatile Thread heartbeater;

    GossipNode(PeerInfo self, List<Endpoint> seeds) throws SocketException {
        this.self = self;
        this.seeds = List.copyOf(seeds);
        this.socket = new DatagramSocket(self.port());
        logger.info("Gossip node " + self.nodeId() + " listening on " + self.endpoint().displayAddress());
    }

    void start() {
        Thread.ofVirtual().name("receiver").start(this::receiveLoop);
        heartbeater = Thread.ofVirtual().name("heartbeater").start(this::heartbeatLoop);
    }

    void close() {
        socket.close();
        Thread sender = heartbeater;
        if (sender != null) sender.interrupt();
    }

    private void heartbeatLoop() {
        while (!socket.isClosed()) {
            try {
                pruneExpiredPeers();
                displayPeers();
                sendHeartbeatRound();
                long jitterMillis = ThreadLocalRandom.current().nextLong(500);
                Thread.sleep(HEARTBEAT_INTERVAL.toMillis() + jitterMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void sendHeartbeatRound() {
        List<Endpoint> candidates = new ArrayList<>(seeds);
        for (PeerState state : peers.values()) {
            candidates.add(state.peer().endpoint());
        }
        Set<Endpoint> uniqueCandidates = new LinkedHashSet<>(candidates);
        candidates = new ArrayList<>(uniqueCandidates);
        Collections.shuffle(candidates, ThreadLocalRandom.current());
        List<Endpoint> targets = candidates.subList(0, Math.min(FANOUT, candidates.size()));
        if (targets.isEmpty()) return;

        byte[] payload = createHeartbeatPayload();
        for (Endpoint target : targets) {
            try {
                DatagramPacket packet = new DatagramPacket(payload, payload.length,
                        InetAddress.getByName(target.host()), target.port());
                socket.send(packet);
            } catch (IOException | IllegalArgumentException e) {
                logger.fine("Could not send heartbeat to " + target.displayAddress() + ": " + e.getMessage());
            }
        }
    }

    private byte[] createHeartbeatPayload() {
        List<PeerState> knownPeers = new ArrayList<>(peers.values());
        Collections.shuffle(knownPeers, ThreadLocalRandom.current());

        Map<String, PeerInfo> rumorMap = new LinkedHashMap<>();
        rumorMap.put(self.nodeId(), self);
        for (PeerState state : knownPeers) {
            if (rumorMap.size() >= MAX_GOSSIP_PEERS) break;
            rumorMap.putIfAbsent(state.peer().nodeId(), state.peer());
        }

        List<PeerInfo> rumors = new ArrayList<>(rumorMap.values());
        while (true) {
            Heartbeat heartbeat = new Heartbeat(self.nodeId(), incarnationId, sequence.incrementAndGet(), rumors);
            byte[] payload = gson.toJson(heartbeat).getBytes(StandardCharsets.UTF_8);
            if (payload.length <= MAX_DATAGRAM_BYTES || rumors.size() == 1) return payload;
            rumors.remove(rumors.size() - 1);
        }
    }

    private void receiveLoop() {
        byte[] buffer = new byte[MAX_DATAGRAM_BYTES + 1];
        logger.info("Waiting for peer heartbeats...");

        while (!socket.isClosed()) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                if (packet.getLength() > MAX_DATAGRAM_BYTES) {
                    logger.fine("Ignoring oversized heartbeat from " + packet.getAddress());
                    continue;
                }

                String json = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                Heartbeat heartbeat = gson.fromJson(json, Heartbeat.class);
                if (!isValid(heartbeat)) {
                    logger.fine("Ignoring malformed heartbeat from " + packet.getAddress());
                    continue;
                }
                if (self.nodeId().equals(heartbeat.nodeId())) continue;

                long now = System.nanoTime();
                PeerInfo directPeer = announcedSelf(heartbeat);
                if (directPeer == null) {
                    directPeer = new PeerInfo(heartbeat.nodeId(), packet.getAddress().getHostAddress(), packet.getPort());
                }
                observeDirectHeartbeat(directPeer, heartbeat, now);

                int accepted = 0;
                for (PeerInfo rumor : heartbeat.peers()) {
                    if (rumor == null || !rumor.isValid() || rumor.nodeId().equals(self.nodeId())) continue;
                    if (rumor.nodeId().equals(heartbeat.nodeId())) continue;
                    if (accepted++ >= MAX_GOSSIP_PEERS) break;
                    observeRumor(rumor, now);
                }
            } catch (SocketException e) {
                if (!socket.isClosed()) logger.warning("Socket error receiving heartbeat: " + e.getMessage());
            } catch (IOException e) {
                logger.warning("Could not receive heartbeat: " + e.getMessage());
            } catch (RuntimeException e) {
                logger.fine("Ignoring malformed heartbeat: " + e.getMessage());
            }
        }
    }

    private boolean isValid(Heartbeat heartbeat) {
        return heartbeat != null
                && heartbeat.nodeId() != null && !heartbeat.nodeId().isBlank() && heartbeat.nodeId().length() <= 64
                && heartbeat.incarnationId() != null && !heartbeat.incarnationId().isBlank()
                && heartbeat.incarnationId().length() <= 64
                && heartbeat.sequence() > 0
                && heartbeat.peers() != null && heartbeat.peers().size() <= MAX_GOSSIP_PEERS;
    }

    private PeerInfo announcedSelf(Heartbeat heartbeat) {
        return heartbeat.peers().stream()
                .filter(peer -> peer != null && peer.nodeId() != null
                        && peer.nodeId().equals(heartbeat.nodeId()) && peer.isValid())
                .findFirst()
                .orElse(null);
    }

    private void observeDirectHeartbeat(PeerInfo peer, Heartbeat heartbeat, long now) {
        PeerState old = peers.get(peer.nodeId());
        if (old != null && heartbeat.incarnationId().equals(old.incarnationId())
                && heartbeat.sequence() <= old.sequence()) {
            return;
        }
        if (old == null && !makeRoom(now)) return;
        peers.put(peer.nodeId(), new PeerState(peer, heartbeat.incarnationId(), heartbeat.sequence(), now, now));
    }

    private void observeRumor(PeerInfo rumor, long now) {
        PeerState old = peers.get(rumor.nodeId());
        if (old == null) {
            if (!makeRoom(now)) return;
            peers.putIfAbsent(rumor.nodeId(), new PeerState(rumor, null, 0, 0, now));
        } else {
            peers.replace(rumor.nodeId(), old, old.withRumor(rumor, now));
        }
    }

    private boolean makeRoom(long now) {
        if (peers.size() < MAX_KNOWN_PEERS) return true;
        pruneExpiredPeers(now);
        if (peers.size() < MAX_KNOWN_PEERS) return true;

        PeerState oldestUnconfirmedOrDead = peers.values().stream()
                .filter(state -> state.lastSeenNanos() == 0
                        || now - state.lastSeenNanos() > deadAfterNanos())
                .min((left, right) -> Long.compare(left.lastHeardAboutNanos(), right.lastHeardAboutNanos()))
                .orElse(null);
        return oldestUnconfirmedOrDead != null && peers.remove(oldestUnconfirmedOrDead.peer().nodeId(), oldestUnconfirmedOrDead);
    }

    private void pruneExpiredPeers() {
        pruneExpiredPeers(System.nanoTime());
    }

    private void pruneExpiredPeers(long now) {
        for (Map.Entry<String, PeerState> entry : peers.entrySet()) {
            if (now - entry.getValue().lastHeardAboutNanos() > MEMBERSHIP_RETENTION.toNanos()) {
                peers.remove(entry.getKey(), entry.getValue());
            }
        }
    }

    private void displayPeers() {
        long now = System.nanoTime();
        System.out.print("\033[H\033[2J");
        System.out.flush();
        System.out.println("==========================================================");
        System.out.printf("  GOSSIP NODE | %s | %s%n", self.nodeId(), self.endpoint().displayAddress());
        System.out.println("==========================================================");
        System.out.printf("  %-24s | %-15s | %-10s%n", "PEER", "LAST SEEN", "STATUS");
        System.out.println("----------------------------------------------------------");

        if (peers.isEmpty()) {
            System.out.println("  (no peers)");
            return;
        }
        peers.values().stream()
                .sorted((left, right) -> left.peer().nodeId().compareTo(right.peer().nodeId()))
                .forEach(state -> displayPeer(state, now));
    }

    private void displayPeer(PeerState state, long now) {
        String status;
        String lastSeen;
        if (state.lastSeenNanos() == 0) {
            status = "DISCOVERED";
            lastSeen = "never";
        } else {
            long age = Math.max(0, now - state.lastSeenNanos());
            status = age > deadAfterNanos() ? "DEAD"
                    : age > suspectAfterNanos() ? "SUSPECT" : "ALIVE";
            lastSeen = formatDuration(age);
        }
        String shortId = state.peer().nodeId().length() > 12
                ? state.peer().nodeId().substring(0, 12) : state.peer().nodeId();
        System.out.printf("  %-24s | %-15s | %-10s%n", shortId, lastSeen, status);
    }

    private long suspectAfterNanos() {
        return Math.max(1, (peers.size() + FANOUT - 1) / FANOUT) * BASE_SUSPECT_AFTER.toNanos();
    }

    private long deadAfterNanos() {
        return Math.max(1, (peers.size() + FANOUT - 1) / FANOUT) * BASE_DEAD_AFTER.toNanos();
    }

    private String formatDuration(long nanos) {
        long millis = Duration.ofNanos(nanos).toMillis();
        return millis < 1000 ? millis + "ms ago" : String.format("%.2fs ago", millis / 1000.0);
    }
}
