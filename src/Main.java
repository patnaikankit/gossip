import com.google.gson.Gson;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

class Constants {
    public static final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(500);
    public static final Duration DEAD_PEER_THRESHOLD = Duration.ofMillis(2500);
    public static final int BUFFER_SIZE = 65_507;
}

record Peer(String ip, int port, long lastSeen) {}
record HeartBeat(List<Peer> knownPeers) {}

class GossipNode {
    private static final Logger logger = Logger.getLogger(GossipNode.class.getName());
    private final Gson gson = new Gson();

    private final String localIp;
    private final int localPort;
    private final ConcurrentHashMap<String, Peer> peers = new ConcurrentHashMap<>();
    private final DatagramSocket socket;

    public GossipNode(int port, String advertisedIp) throws SocketException {
        this.localIp = advertisedIp;
        this.localPort = port;
        this.socket = new DatagramSocket(port);
        logger.info("Gossip node created and initialized on " + localIp + ":" + localPort);
    }

    public void start(String[] seeds) {
        Thread.ofVirtual().name("receiver").start(this::receiveLoop);
        Thread.ofVirtual().name("heartbeater").start(this::heartBeat);

        for (String seed : seeds) {
            if (seed.isBlank()) continue;
            try {
                HostPort endpoint = parseEndpoint(seed);
                sendHeartbeatTo(endpoint.host(), endpoint.port());
            } catch (IllegalArgumentException e) {
                logger.warning("Ignoring invalid seed '" + seed + "': " + e.getMessage());
            }
        }
    }

    public void close() {
        socket.close();
    }

    private void heartBeat() {
        while (!socket.isClosed()) {
            try {
                displayPeers();

                for (Peer peer : peers.values()) {
                    sendHeartbeatTo(peer.ip(), peer.port());
                }

                int jitter = ThreadLocalRandom.current().nextInt(500);
                Thread.sleep(Constants.HEARTBEAT_INTERVAL.toMillis() + jitter);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendHeartbeatTo(String ip, int port) {
        try {
            List<Peer> peerList = new ArrayList<>(peers.values());
            String payload = gson.toJson(new HeartBeat(peerList));
            byte[] buffer = payload.getBytes(StandardCharsets.UTF_8);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, InetAddress.getByName(ip), port);
            socket.send(packet);
        } catch (IOException e) {
            logger.fine("Could not send heartbeat to " + formatEndpoint(ip, port) + ": " + e.getMessage());
        }
    }

    private void displayPeers() {
        long now = System.currentTimeMillis();

        System.out.print("\033[H\033[2J");
        System.out.flush();

        System.out.println("==========================================================");
        System.out.printf("  GOSSIP CLUSTER STATUS | Node: %s:%-5d %n", localIp, localPort);
        System.out.println("==========================================================");
        System.out.printf("  %-22s | %-15s | %-8s%n", "PEER ADDRESS", "LAST SEEN", "STATUS");
        System.out.println("----------------------------------------------------------");

        if (peers.isEmpty()) {
            System.out.println("  ...");
        } else {
            peers.forEach((key, peer) -> {
                long diff = peer.lastSeen() == 0 ? Long.MAX_VALUE : now - peer.lastSeen();
                boolean isDead = diff > Constants.DEAD_PEER_THRESHOLD.toMillis();

                String timeStr = peer.lastSeen() == 0 ? "never" : formatDuration(diff);
                String statusColor = isDead ? "\u001B[31m" : "\u001B[33m";
                String resetColor = "\u001B[0m";
                String status = isDead ? "DEAD" : "ALIVE";

                System.out.printf("  %-22s | %-15s | %s%-8s%s%n", key, timeStr, statusColor, status, resetColor);
            });
        }
    }

    private String formatDuration(long ms) {
        if (ms < 1000) {
            return ms + "ms ago";
        }
        return String.format("%.2fs ago", ms / 1000.0);
    }

    private void receiveLoop() {
        byte[] buffer = new byte[Constants.BUFFER_SIZE];
        logger.info("Listening for peers...");

        while (!socket.isClosed()) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String rawJson = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                HeartBeat heartbeat = gson.fromJson(rawJson, HeartBeat.class);
                if (heartbeat == null || heartbeat.knownPeers() == null) {
                    logger.fine("Ignoring malformed heartbeat from " + packet.getAddress());
                    continue;
                }

                InetAddress senderAddress = packet.getAddress();
                String senderIp = senderAddress.getHostAddress();
                int senderPort = packet.getPort();
                if (!isLocalEndpoint(senderAddress, senderPort)) {
                    observePeer(senderIp, senderPort);
                }

                for (Peer peer : heartbeat.knownPeers()) {
                    if (peer == null || peer.ip() == null || peer.ip().isBlank()
                            || peer.port() < 1 || peer.port() > 65535) {
                        continue;
                    }

                    try {
                        InetAddress peerAddress = InetAddress.getByName(peer.ip());
                        if (!isLocalEndpoint(peerAddress, peer.port())) {
                            // The timestamp belongs to the node that sent this list. Only a direct
                            // heartbeat can establish when this node last heard from a peer.
                            peers.putIfAbsent(formatEndpoint(peer.ip(), peer.port()),
                                    new Peer(peer.ip(), peer.port(), 0));
                        }
                    } catch (IOException e) {
                        logger.fine("Ignoring peer with invalid address '" + peer.ip() + "'");
                    }
                }
            } catch (SocketException e) {
                if (!socket.isClosed()) {
                    logger.warning("Socket error while receiving heartbeat: " + e.getMessage());
                }
            } catch (IOException e) {
                logger.warning("Error receiving heartbeat: " + e.getMessage());
            } catch (RuntimeException e) {
                logger.fine("Ignoring malformed heartbeat: " + e.getMessage());
            }
        }
    }

    private void observePeer(String ip, int port) {
        String key = formatEndpoint(ip, port);
        peers.put(key, new Peer(ip, port, System.currentTimeMillis()));
    }

    private boolean isLocalEndpoint(InetAddress address, int port) {
        if (port != localPort) return false;
        try {
            InetAddress advertisedAddress = InetAddress.getByName(localIp);
            return address.equals(advertisedAddress) || address.isLoopbackAddress() || address.isAnyLocalAddress();
        } catch (IOException e) {
            return false;
        }
    }

    private static String formatEndpoint(String host, int port) {
        return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
    }

    private static HostPort parseEndpoint(String endpoint) {
        String host;
        String portText;

        if (endpoint.startsWith("[")) {
            int closingBracket = endpoint.indexOf(']');
            if (closingBracket < 0 || closingBracket + 1 >= endpoint.length()
                    || endpoint.charAt(closingBracket + 1) != ':') {
                throw new IllegalArgumentException("IPv6 seeds must use [address]:port format");
            }
            host = endpoint.substring(1, closingBracket);
            portText = endpoint.substring(closingBracket + 2);
        } else {
            int separator = endpoint.lastIndexOf(':');
            if (separator <= 0 || endpoint.indexOf(':') != separator) {
                throw new IllegalArgumentException("Seeds must use host:port format (bracket IPv6 addresses)");
            }
            host = endpoint.substring(0, separator);
            portText = endpoint.substring(separator + 1);
        }

        try {
            int port = Integer.parseInt(portText);
            if (host.isBlank() || port < 1 || port > 65535) {
                throw new IllegalArgumentException("host or port is out of range");
            }
            return new HostPort(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port must be a number from 1 to 65535", e);
        }
    }

    private record HostPort(String host, int port) {}
}

public class Main {
    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] %5$s%n");

        int port = Integer.parseInt(System.getProperty("port", "8000"));
        String advertisedIp = System.getProperty("host", "127.0.0.1");
        String seedsProperty = System.getProperty("seeds", "");
        String[] seeds = seedsProperty.isBlank() ? new String[0] : seedsProperty.split(",");

        GossipNode node = new GossipNode(port, advertisedIp);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "gossip-shutdown"));
        node.start(seeds);

        Thread.currentThread().join();
    }
}
