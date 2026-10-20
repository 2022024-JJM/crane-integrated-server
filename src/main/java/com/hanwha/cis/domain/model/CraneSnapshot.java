package com.hanwha.cis.domain.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 크레인 1대의 특정 시점 전체 상태(불변). Redis 해시 값, WebSocket 메시지, 이력 행의 state JSON이 모두 이 형태다.
 *
 * @param snapshotAt     마지막으로 반영된 데이터 시각(occurredAt). 태그를 하나도 받지 못했으면 null
 * @param lastReceivedAt 마지막 수신 벽시계 시각
 * @param online         staleThreshold 안에 수신이 있었는지
 * @param tags           태그 이름순 정렬
 */
public record CraneSnapshot(
        String craneId,
        String stationId,
        String bay,
        Instant snapshotAt,
        Instant lastReceivedAt,
        boolean online,
        Map<String, TagState> tags) {

    public CraneSnapshot {
        Objects.requireNonNull(craneId, "craneId");
        tags = Collections.unmodifiableMap(new TreeMap<>(tags == null ? Map.of() : tags));
    }

    /** 태그 이름 → 값만 추린 맵(이력 프레임용). */
    public Map<String, TagValue> values() {
        Map<String, TagValue> values = new LinkedHashMap<>();
        tags.forEach((name, state) -> values.put(name, state.value()));
        return Collections.unmodifiableMap(values);
    }

    /** 값(과 online)만 이어붙인 문자열. 직전 diff 행과 비교해 상태 변화 여부를 판정한다. */
    public String fingerprint() {
        StringBuilder sb = new StringBuilder(tags.size() * 24 + 16);
        sb.append("online=").append(online);
        tags.forEach((name, state) -> sb.append(';').append(name).append('=').append(state.value().raw()));
        return sb.toString();
    }
}
