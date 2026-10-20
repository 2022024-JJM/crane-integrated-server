package com.hanwha.cis.application.port.out;

import java.time.Instant;

/** 지금까지 본 크레인 목록(crane 테이블). */
public interface CraneRegistryPort {

    /** 처음 보면 insert, 이미 있으면 last_seen_at 갱신. 비동기(쓰기 큐)여도 된다. */
    void upsertSeen(String craneId, String stationId, String bay, Instant seenAt);

    boolean exists(String craneId);
}
