package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.PushSnapshotsUseCase;
import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.application.port.out.SnapshotPublisherPort;
import com.hanwha.cis.application.support.Durations;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 값이 변한 크레인(dirty)과 online 상태가 바뀐 크레인의 스냅샷을 저장소에 쓰고 발행한다. 푸시 스케줄러 스레드 하나만 호출한다.
 * online 전환(복귀·끊김)은 틱마다 종류별 한 줄로 로그에 남긴다.
 */
public class SnapshotPushService implements PushSnapshotsUseCase {

    private static final Logger log = LoggerFactory.getLogger(SnapshotPushService.class);
    private static final int MAX_LOGGED_CRANES = 20;

    private final CraneStateRegistry registry;
    private final CraneStateStorePort store;
    private final SnapshotPublisherPort publisher;
    private final Clock clock;
    private final Duration staleThreshold;
    /** 마지막으로 발행한 online 값. 수신이 끊겨 offline이 되는 전이는 dirty로 잡히지 않으므로 따로 본다. */
    private final Map<String, Boolean> lastOnline = new HashMap<>();
    /** offline으로 바뀐(감지한) 시각. 복귀 로그에 offline 지속 시간을 남기기 위함. */
    private final Map<String, Instant> offlineSince = new HashMap<>();

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
        Transitions transitions = new Transitions();
        for (CraneState state : registry.all()) {
            boolean online = state.isOnline(now, staleThreshold);
            Boolean previous = lastOnline.put(state.craneId(), online);
            if (online) {
                transitions.online++;
            }
            if (previous == null || previous != online) {
                ids.add(state.craneId());
                transitions.add(state.craneId(), previous, online, now);
            }
        }
        transitions.report(registry.size());
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

    /** 한 틱의 online 전환을 모아 종류별로 한 줄씩 남긴다(Kafka가 끊기면 전 크레인이 한꺼번에 바뀌므로). */
    private final class Transitions {
        private final List<String> cameOnline = new ArrayList<>();
        private final List<String> wentOffline = new ArrayList<>();
        private final List<String> offlineAtStart = new ArrayList<>();
        private int online;

        void add(String craneId, Boolean previous, boolean nowOnline, Instant now) {
            if (nowOnline) {
                Instant since = offlineSince.remove(craneId);
                cameOnline.add(since == null ? craneId
                        : craneId + " (offline " + Durations.format(Duration.between(since, now)) + ")");
            } else if (previous == null) {
                // 기동 전부터 끊겨 있던 크레인은 언제부터인지 모르므로 복귀 때 지속 시간을 붙이지 않는다.
                offlineAtStart.add(craneId);
            } else {
                offlineSince.put(craneId, now);
                wentOffline.add(craneId);
            }
        }

        void report(int total) {
            if (!wentOffline.isEmpty()) {
                log.warn("Cranes offline, no data for {} ({}/{} online): {}", Durations.format(staleThreshold),
                        online, total, names(wentOffline));
            }
            if (!offlineAtStart.isEmpty()) {
                log.info("Cranes offline at startup, waiting for data ({}/{} online): {}", online, total,
                        names(offlineAtStart));
            }
            if (!cameOnline.isEmpty()) {
                log.info("Cranes online ({}/{} online): {}", online, total, names(cameOnline));
            }
        }

        private static String names(List<String> cranes) {
            List<String> sorted = cranes.stream().sorted().toList();
            if (sorted.size() <= MAX_LOGGED_CRANES) {
                return String.join(", ", sorted);
            }
            return String.join(", ", sorted.subList(0, MAX_LOGGED_CRANES))
                    + " ... (+" + (sorted.size() - MAX_LOGGED_CRANES) + " more)";
        }
    }
}
