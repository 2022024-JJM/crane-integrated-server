package com.hanwha.cis.domain.model;

import java.time.Instant;
import java.util.Objects;

/** Kafka에서 받은 태그 1건을 검증·정규화한 결과. scope는 받은 토픽의 범위다. */
public record TagReading(CraneScope scope, String stationId, TagKey tagKey, TagValue value, int quality,
        Instant occurredAt) {

    public TagReading {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(tagKey, "tagKey");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }
}
