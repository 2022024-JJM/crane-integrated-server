package com.hanwha.cis.application.service;

import com.hanwha.cis.domain.model.CraneScope;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** 수신 카운터. scope(indoor/outdoor)마다 하나씩 두고, Micrometer 바인딩과 요약 로그가 읽는다. */
public final class IngestStats {

    public enum SkipReason {
        PARSE("parse"), MISMATCH("mismatch"), LATE("late");

        private final String tag;

        SkipReason(String tag) {
            this.tag = tag;
        }

        public String tag() {
            return tag;
        }
    }

    /** 잘못된 메시지는 아니지만 크레인 상태에 넣지 않은 것(outdoor만 해당). */
    public enum IgnoreReason {
        /** 크레인 슬롯 밖의 공통 블록(시각, 레일, 장애물 등). */
        COMMON("common"),
        /** Crane_ID가 0이거나 아직 모르는 슬롯의 태그. */
        UNASSIGNED("unassigned");

        private final String tag;

        IgnoreReason(String tag) {
            this.tag = tag;
        }

        public String tag() {
            return tag;
        }
    }

    private final CraneScope scope;
    private final LongAdder received = new LongAdder();
    private final LongAdder changed = new LongAdder();
    private final LongAdder nulls = new LongAdder();
    private final Map<SkipReason, LongAdder> skipped = new EnumMap<>(SkipReason.class);
    private final Map<IgnoreReason, LongAdder> ignored = new EnumMap<>(IgnoreReason.class);

    public IngestStats(CraneScope scope) {
        this.scope = scope;
        for (SkipReason reason : SkipReason.values()) {
            skipped.put(reason, new LongAdder());
        }
        for (IgnoreReason reason : IgnoreReason.values()) {
            ignored.put(reason, new LongAdder());
        }
    }

    public CraneScope scope() {
        return scope;
    }

    public void onReceived() {
        received.increment();
    }

    public void onChanged() {
        changed.increment();
    }

    /** 값 없음(null)으로 받은 태그. 상태에는 null로 들어간다. */
    public void onNull() {
        nulls.increment();
    }

    public void onSkipped(SkipReason reason) {
        skipped.get(reason).increment();
    }

    public void onIgnored(IgnoreReason reason) {
        ignored.get(reason).increment();
    }

    public long received() {
        return received.sum();
    }

    public long changed() {
        return changed.sum();
    }

    public long nulls() {
        return nulls.sum();
    }

    public long skipped(SkipReason reason) {
        return skipped.get(reason).sum();
    }

    public long skippedTotal() {
        return skipped.values().stream().mapToLong(LongAdder::sum).sum();
    }

    public long ignored(IgnoreReason reason) {
        return ignored.get(reason).sum();
    }
}
