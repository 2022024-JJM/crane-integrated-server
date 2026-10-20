package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.PushSnapshotsUseCase;
import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.application.port.out.SnapshotPublisherPort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.CraneState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 값이 변한 크레인(dirty)과 online 상태가 바뀐 크레인의 스냅샷을 저장소에 쓰고 발행한다. 푸시 스케줄러 스레드 하나만 호출한다.
 */
public class SnapshotPushService implements PushSnapshotsUseCase {

    private final CraneStateRegistry registry;
    private final CraneStateStorePort store;
    private final SnapshotPublisherPort publisher;
    private final Clock clock;
    private final Duration staleThreshold;
    /** 마지막으로 발행한 online 값. 수신이 끊겨 offline이 되는 전이는 dirty로 잡히지 않으므로 따로 본다. */
    private final Map<String, Boolean> lastOnline = new HashMap<>();

    public SnapshotPushService(CraneStateRegistry registry, CraneStateStorePort store,
            SnapshotPublisherPort publisher, Clock clock, Duration staleThreshold) {
        this.registry = registry;
        this.store = store;
        this.publisher = publisher;
        this.clock = clock;
        this.staleThreshold = staleThreshold;
    }

    @Override
    public int pushDirty() {
        Instant now = clock.instant();
        Set<String> ids = new LinkedHashSet<>(registry.drainDirty());
        for (CraneState state : registry.all()) {
            boolean online = state.isOnline(now, staleThreshold);
            Boolean previous = lastOnline.put(state.craneId(), online);
            if (previous == null || previous != online) {
                ids.add(state.craneId());
            }
        }
        if (ids.isEmpty()) {
            return 0;
        }
        List<CraneSnapshot> snapshots = new ArrayList<>(ids.size());
        for (String id : ids) {
            registry.find(id)
                    .map(state -> state.snapshot(now, staleThreshold))
                    .filter(snapshot -> snapshot.snapshotAt() != null)
                    .ifPresent(snapshots::add);
        }
        if (snapshots.isEmpty()) {
            return 0;
        }
        try {
            store.save(snapshots);
            publisher.publish(snapshots);
        } catch (RuntimeException e) {
            registry.markDirty(ids);
            throw e;
        }
        return snapshots.size();
    }
}
