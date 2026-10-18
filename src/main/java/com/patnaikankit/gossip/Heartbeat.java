package com.patnaikankit.gossip;

import java.util.List;

record Heartbeat(String nodeId, String incarnationId, long sequence, List<PeerInfo> peers) {}
