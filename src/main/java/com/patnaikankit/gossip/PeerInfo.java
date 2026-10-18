package com.patnaikankit.gossip;

record PeerInfo(String nodeId, String host, int port) {
    boolean isValid() {
        return nodeId != null && !nodeId.isBlank() && nodeId.length() <= 64
                && host != null && !host.isBlank() && host.length() <= 253
                && host.chars().noneMatch(Character::isWhitespace)
                && port >= 1 && port <= 65535;
    }

    Endpoint endpoint() {
        return new Endpoint(host, port);
    }
}
