package com.hanwha.cis.domain.model;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 크레인 1대의 최신 상태(가변 집합체). Kafka 리스너 스레드가 {@link #apply}를, 스케줄러 스레드가 {@link #snapshot}을
 * 호출하므로 인스턴스 단위로 동기화한다.
 */
public final class CraneState {

    private final String craneId;
    private final CraneScope scope;
    private String stationId;
    private String bay;
    private final Map<String, TagState> tags = new HashMap<>();
    /** 마지막으로 반영한 occurredAt(데이터 시각). */
    private Instant snapshotAt;
    /** 마지막으로 수신한 벽시계 시각. 30초 stale 판정 기준. */
    private Instant lastReceivedAt;

    private CraneState(String craneId, CraneScope scope) {
        this.craneId = Objects.requireNonNull(craneId, "craneId");
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    public static CraneState of(String craneId, CraneScope scope) {
        return new CraneState(craneId, scope);
    }

    /** Redis 등에 저장된 스냅샷에서 복원한다(재시작 직후 "모든 태그 변경" 오판 방지). */
    public static CraneState fromSnapshot(CraneSnapshot snapshot) {
        CraneState state = new CraneState(snapshot.craneId(), snapshot.scope());
        state.stationId = snapshot.stationId();
        state.bay = snapshot.bay();
        state.snapshotAt = snapshot.snapshotAt();
        state.lastReceivedAt = snapshot.lastReceivedAt();
        state.tags.putAll(snapshot.tags());
        return state;
    }

    public String craneId() {
        return craneId;
    }

    public CraneScope scope() {
        return scope;
    }

    /**
     * 태그 1건을 반영한다.
     *
     * @param reading    정규화된 태그 값
     * @param receivedAt 수신 벽시계 시각
     */
    public synchronized ApplyResult apply(TagReading reading, Instant receivedAt) {
        if (!craneId.equals(reading.tagKey().craneId())) {
            throw new IllegalArgumentException(
                    "reading for " + reading.tagKey().craneId() + " applied to " + craneId);
        }
        if (scope != reading.scope()) {
            throw new IllegalArgumentException(
                    reading.scope().id() + " reading applied to " + scope.id() + " crane " + craneId);
        }
        String tagName = reading.tagKey().tagName();
        TagState previous = tags.get(tagName);
        if (previous != null && reading.occurredAt().isBefore(previous.timestamp())) {
            return ApplyResult.LATE_IGNORED;
        }
        stationId = reading.stationId();
        bay = reading.tagKey().bay();
        if (lastReceivedAt == null || receivedAt.isAfter(lastReceivedAt)) {
            lastReceivedAt = receivedAt;
        }
        if (snapshotAt == null || reading.occurredAt().isAfter(snapshotAt)) {
            snapshotAt = reading.occurredAt();
        }
        tags.put(tagName, new TagState(reading.value(), reading.quality(), reading.occurredAt()));
        boolean changed = previous == null || !previous.value().equals(reading.value());
        return changed ? ApplyResult.CHANGED : ApplyResult.UNCHANGED;
    }

    /**
     * 태그 전부를 이 값들로 바꾼다(이전 태그는 지운다). 외업 크레인이 다른 슬롯으로 옮겨 태그 이름이 바뀔 때 쓴다.
     * 한 번에 바꾸므로 스냅샷에 빈 태그나 두 슬롯의 태그가 섞여 보이지 않는다.
     */
    public synchronized void replaceTags(List<TagReading> readings, Instant receivedAt) {
        tags.clear();
        for (TagReading reading : readings) {
            apply(reading, receivedAt);
        }
    }

    public synchronized boolean isOnline(Instant now, Duration staleThreshold) {
        return lastReceivedAt != null && !lastReceivedAt.plus(staleThreshold).isBefore(now);
    }

    public synchronized Instant snapshotAt() {
        return snapshotAt;
    }

    public synchronized CraneSnapshot snapshot(Instant now, Duration staleThreshold) {
        return new CraneSnapshot(craneId, scope, stationId, bay, snapshotAt, lastReceivedAt,
                isOnline(now, staleThreshold), tags);
    }
}
