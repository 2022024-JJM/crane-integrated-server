package com.hanwha.cis.support;

import com.hanwha.cis.application.port.out.CraneRegistryPort;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryCraneRegistryPort implements CraneRegistryPort {

    public record Seen(String craneId, String stationId, String bay, Instant seenAt) {
    }

    private final Set<String> known = ConcurrentHashMap.newKeySet();
    private final List<Seen> upserts = new CopyOnWriteArrayList<>();

    @Override
    public void upsertSeen(String craneId, String stationId, String bay, Instant seenAt) {
        known.add(craneId);
        upserts.add(new Seen(craneId, stationId, bay, seenAt));
    }

    @Override
    public boolean exists(String craneId) {
        return known.contains(craneId);
    }

    public List<Seen> upserts() {
        return upserts;
    }
}
