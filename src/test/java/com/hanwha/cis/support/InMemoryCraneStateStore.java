package com.hanwha.cis.support;

import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Redis 해시를 대신하는 메모리 저장소. */
public class InMemoryCraneStateStore implements CraneStateStorePort {

    private final Map<String, CraneSnapshot> snapshots = new ConcurrentHashMap<>();

    @Override
    public void save(List<CraneSnapshot> toSave) {
        toSave.forEach(s -> snapshots.put(s.craneId(), s));
    }

    @Override
    public List<CraneSnapshot> findAll() {
        return List.copyOf(snapshots.values());
    }

    @Override
    public Optional<CraneSnapshot> find(String craneId) {
        return Optional.ofNullable(snapshots.get(craneId));
    }

    public void clear() {
        snapshots.clear();
    }
}
