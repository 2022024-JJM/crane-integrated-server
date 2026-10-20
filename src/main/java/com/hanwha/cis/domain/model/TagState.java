package com.hanwha.cis.domain.model;

import java.time.Instant;
import java.util.Objects;

/** 크레인 스냅샷 안의 태그 1개 상태. quality는 저장·노출만 하고 변경 감지에는 쓰지 않는다. */
public record TagState(TagValue value, int quality, Instant timestamp) {

    public TagState {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(timestamp, "timestamp");
    }
}
