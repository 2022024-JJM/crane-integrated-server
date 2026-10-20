package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.domain.model.ApplyResult;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.CraneState;
import com.hanwha.cis.domain.model.TagReading;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 모든 크레인의 메모리 상태와 "푸시가 필요한 크레인" 집합(dirty). Kafka 스레드가 apply, 푸시 스레드가 drainDirty를 부른다.
 */
public class CraneStateRegistry {

    private static final Logger log = LoggerFactory.getLogger(CraneStateRegistry.class);

    private final ConcurrentHashMap<String, CraneState> states = new ConcurrentHashMap<>();
    private final Set<String> dirty = ConcurrentHashMap.newKeySet();
    private final CraneStateStorePort store;

    public CraneStateRegistry(CraneStateStorePort store) {
        this.store = store;
    }

    /** 기동 시 1회: 저장소의 스냅샷으로 메모리를 채운다. 저장소 장애면 빈 상태로 시작한다. */
    public void restore() {
        List<CraneSnapshot> saved;
        try {
            saved = store.findAll();
        } catch (RuntimeException e) {
            log.warn("Could not restore crane states from store, starting empty: {}", e.toString());
            return;
        }
        int restored = 0;
        for (CraneSnapshot snapshot : saved) {
            if (snapshot.snapshotAt() == null) {
                continue;
            }
            if (states.putIfAbsent(snapshot.craneId(), CraneState.fromSnapshot(snapshot)) == null) {
                restored++;
            }
        }
        log.info("Restored {} crane state(s) from store", restored);
    }

    public ApplyResult apply(TagReading reading, Instant receivedAt) {
        CraneState state = states.computeIfAbsent(reading.tagKey().craneId(), CraneState::of);
        ApplyResult result = state.apply(reading, receivedAt);
        if (result == ApplyResult.CHANGED) {
            dirty.add(state.craneId());
        }
        return result;
    }

    /** dirty 집합을 비우며 그 내용을 돌려준다. 비운 뒤 들어온 변경은 다음 호출에 잡힌다. */
    public List<String> drainDirty() {
        List<String> ids = new ArrayList<>();
        for (String id : dirty) {
            if (dirty.remove(id)) {
                ids.add(id);
            }
        }
        return ids;
    }

    /** 푸시 실패 시 되돌려 놓는다. */
    public void markDirty(Collection<String> craneIds) {
        dirty.addAll(craneIds);
    }

    public Optional<CraneState> find(String craneId) {
        return Optional.ofNullable(states.get(craneId));
    }

    public Collection<CraneState> all() {
        return Collections.unmodifiableCollection(states.values());
    }

    public int size() {
        return states.size();
    }

    public long onlineCount(Instant now, Duration staleThreshold) {
        return states.values().stream().filter(s -> s.isOnline(now, staleThreshold)).count();
    }

    /** craneId순 스냅샷(태그를 하나라도 받은 크레인만). */
    public List<CraneSnapshot> snapshots(Instant now, Duration staleThreshold) {
        return states.values().stream()
                .map(s -> s.snapshot(now, staleThreshold))
                .filter(s -> s.snapshotAt() != null)
                .sorted(Comparator.comparing(CraneSnapshot::craneId))
                .toList();
    }
}
