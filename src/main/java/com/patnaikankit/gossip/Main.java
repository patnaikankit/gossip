package com.patnaikankit.gossip;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] %5$s%n");

        int port = parsePort(System.getProperty("port", "8000"));
        String host = System.getProperty("host", "127.0.0.1").trim();
        String nodeId = System.getProperty("nodeId", UUID.randomUUID().toString()).trim();
        if (host.isBlank() || host.length() > 253 || host.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("host must be a non-empty hostname or IP address");
        }
        if (nodeId.isBlank() || nodeId.length() > 64 || nodeId.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("nodeId must be 1 to 64 non-whitespace characters");
        }

        String seedsProperty = System.getProperty("seeds", "");
        List<Endpoint> seeds = Arrays.stream(seedsProperty.split(","))
                .map(String::trim)
                .filter(seed -> !seed.isEmpty())
                .map(Endpoint::parse)
                .toList();

        GossipNode node = new GossipNode(new PeerInfo(nodeId, host, port), seeds);
        Runtime.getRuntime().addShutdownHook(new Thread(node::close, "gossip-shutdown"));
        node.start();

        new CountDownLatch(1).await();
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port must be between 1 and 65535");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port must be a number between 1 and 65535", e);
        }
    }
}
