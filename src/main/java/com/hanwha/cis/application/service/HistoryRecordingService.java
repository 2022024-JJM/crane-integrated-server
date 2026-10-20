package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.RecordHistoryUseCase;
import com.hanwha.cis.application.port.out.CraneHistoryPort;
import com.hanwha.cis.application.port.out.CraneHistoryPort.DiffRow;
import com.hanwha.cis.application.port.out.CraneHistoryPort.HistoryRow;
import com.hanwha.cis.application.port.out.CraneRegistryPort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.CraneState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 틱마다 크레인별로: snapshotAt이 전진했으면 crane_history 행, fingerprint(값+online)가 바뀌었으면 crane_history_diff 행.
 * 행의 time은 틱 시각이 아니라 스냅샷의 snapshotAt이다. 이력 스케줄러 스레드 하나만 호출한다.
 */
public class HistoryRecordingService implements RecordHistoryUseCase {

    /** crane.last_seen_at 갱신 주기. */
    static final Duration SEEN_REFRESH = Duration.ofSeconds(60);

    private final CraneStateRegistry registry;
    private final CraneHistoryPort history;
    private final CraneRegistryPort cranes;
    private final Clock clock;
    private final Duration staleThreshold;

    private final Map<String, Instant> lastHistoryAt = new HashMap<>();
    private final Map<String, String> lastDiffFingerprint = new HashMap<>();
    private final Map<String, Instant> lastDiffAt = new HashMap<>();
    private final Map<String, Instant> lastSeenUpsertAt = new HashMap<>();

    public HistoryRecordingService(CraneStateRegistry registry, CraneHistoryPort history,
            CraneRegistryPort cranes, Clock clock, Duration staleThreshold) {
        this.registry = registry;
        this.history = history;
        this.cranes = cranes;
        this.clock = clock;
        this.staleThreshold = staleThreshold;
    }

    @Override
    public void recordTick() {
        Instant now = clock.instant();
        for (CraneState state : registry.all()) {
            CraneSnapshot snapshot = state.snapshot(now, staleThreshold);
            if (snapshot.snapshotAt() == null) {
                continue;
            }
            record(snapshot, now);
        }
    }

    private void record(CraneSnapshot snapshot, Instant now) {
        String craneId = snapshot.craneId();
        Instant snapshotAt = snapshot.snapshotAt();

        Instant previousHistoryAt = lastHistoryAt.get(craneId);
        if (previousHistoryAt == null || snapshotAt.isAfter(previousHistoryAt)) {
            history.enqueueHistory(new HistoryRow(snapshotAt, snapshot));
            lastHistoryAt.put(craneId, snapshotAt);
        }

        String fingerprint = snapshot.fingerprint();
        if (!fingerprint.equals(lastDiffFingerprint.get(craneId))) {
            // 수신 없이 online만 바뀐 경우 snapshotAt은 그대로라 (crane_id, time)이 겹친다 → 감지 시각을 쓴다.
            Instant previousDiffAt = lastDiffAt.get(craneId);
            Instant time = previousDiffAt == null || snapshotAt.isAfter(previousDiffAt) ? snapshotAt : now;
            history.enqueueDiff(new DiffRow(time, snapshot.online(), snapshot));
            lastDiffFingerprint.put(craneId, fingerprint);
            lastDiffAt.put(craneId, time);
        }

        Instant previousUpsertAt = lastSeenUpsertAt.get(craneId);
        if (snapshot.online()
                && (previousUpsertAt == null || !previousUpsertAt.plus(SEEN_REFRESH).isAfter(now))) {
            cranes.upsertSeen(craneId, snapshot.stationId(), snapshot.bay(), snapshot.lastReceivedAt());
            lastSeenUpsertAt.put(craneId, now);
        }
    }
}
