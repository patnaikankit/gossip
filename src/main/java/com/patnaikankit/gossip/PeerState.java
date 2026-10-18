package com.patnaikankit.gossip;

record PeerState(
        PeerInfo peer,
        String incarnationId,
        long sequence,
        long lastSeenNanos,
        long lastHeardAboutNanos) {

    PeerState withDirectHeartbeat(PeerInfo updatedPeer, String updatedIncarnation, long updatedSequence, long now) {
        return new PeerState(updatedPeer, updatedIncarnation, updatedSequence, now, now);
    }

    PeerState withRumor(PeerInfo updatedPeer, long now) {
        return new PeerState(updatedPeer, incarnationId, sequence, lastSeenNanos, now);
    }
}
