package com.hanwha.cis.support;

import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.TagKey;
import com.hanwha.cis.domain.model.TagReading;
import com.hanwha.cis.domain.model.TagValue;
import java.time.Instant;

/** 테스트용 TagReading 생성 도우미(indoor). */
public final class IndoorReadings {

    public static final String STATION = "isl-ocean-crane";

    private IndoorReadings() {
    }

    public static TagReading reading(String craneId, String tag, Object value, Instant at) {
        return reading("F5_1_Bay", craneId, tag, value, at);
    }

    public static TagReading reading(String bay, String craneId, String tag, Object value, Instant at) {
        return new TagReading(CraneScope.INDOOR, STATION, new TagKey(bay, craneId, tag), TagValue.of(value), 0, at);
    }
}
