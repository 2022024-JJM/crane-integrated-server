package com.hanwha.cis.application.service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** 수신 카운터. Micrometer 바인딩과 요약 로그가 읽는다. */
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

    private final LongAdder received = new LongAdder();
    private final LongAdder changed = new LongAdder();
    private final Map<SkipReason, LongAdder> skipped = new EnumMap<>(SkipReason.class);

    public IngestStats() {
        for (SkipReason reason : SkipReason.values()) {
            skipped.put(reason, new LongAdder());
        }
    }

    public void onReceived() {
        received.increment();
    }

    public void onChanged() {
        changed.increment();
    }

    public void onSkipped(SkipReason reason) {
        skipped.get(reason).increment();
    }

    public long received() {
        return received.sum();
    }

    public long changed() {
        return changed.sum();
    }

    public long skipped(SkipReason reason) {
        return skipped.get(reason).sum();
    }

    public long skippedTotal() {
        return skipped.values().stream().mapToLong(LongAdder::sum).sum();
    }
}
