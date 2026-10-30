package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.IngestTagReadingUseCase;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import com.hanwha.cis.domain.model.ApplyResult;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.TagKey;
import com.hanwha.cis.domain.model.TagReading;
import com.hanwha.cis.domain.model.TagValue;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 내업(indoor) 원시 이벤트를 검증·정규화해 메모리 상태에 반영하고 카운터를 센다. 문제 메시지는 건너뛰되 분당 일정 건수만 로그에 남긴다.
 * 외업은 {@link OutdoorTagIngestionService}.
 */
public class TagIngestionService implements IngestTagReadingUseCase {

    private static final Logger log = LoggerFactory.getLogger(TagIngestionService.class);
    private static final int MAX_PAYLOAD_SAMPLE = 200;
    private static final int MAX_SKIP_LOGS_PER_MINUTE = 10;

    private final CraneStateRegistry registry;
    private final IngestStats stats;
    private final Clock clock;
    private final RateLimiter skipLogLimiter = new RateLimiter(MAX_SKIP_LOGS_PER_MINUTE);

    public TagIngestionService(CraneStateRegistry registry, IngestStats stats, Clock clock) {
        this.registry = registry;
        this.stats = stats;
        this.clock = clock;
    }

    @Override
    public IngestOutcome ingest(RawTagEvent event) {
        stats.onReceived();
        TagKey tagKey;
        try {
            tagKey = TagKey.parse(event.tagKey());
        } catch (IllegalArgumentException e) {
            return skip(SkipReason.PARSE, e.getMessage(), event);
        }
        if (event.craneId() == null || !tagKey.craneId().equals(event.craneId())) {
            return skip(SkipReason.MISMATCH,
                    "tagKey craneId " + tagKey.craneId() + " != envelope craneId " + event.craneId(), event);
        }
        if (event.recordKey() != null && !event.recordKey().equals(event.craneId())) {
            return skip(SkipReason.MISMATCH,
                    "record key " + event.recordKey() + " != envelope craneId " + event.craneId(), event);
        }
        if (event.occurredAt() == null) {
            return skip(SkipReason.PARSE, "occurredAt is missing", event);
        }
        TagValue value;
        try {
            value = TagValue.of(event.value());
        } catch (IllegalArgumentException e) {
            return skip(SkipReason.PARSE, e.getMessage(), event);
        }
        if (value instanceof TagValue.NullValue) {
            stats.onNull();
        }
        int quality = event.quality() == null ? 0 : event.quality();
        TagReading reading = new TagReading(CraneScope.INDOOR, event.stationId(), tagKey, value, quality,
                Instant.ofEpochMilli(event.occurredAt()));

        ApplyResult result = registry.apply(reading, clock.instant());
        return switch (result) {
            case CHANGED -> {
                stats.onChanged();
                yield IngestOutcome.CHANGED;
            }
            case UNCHANGED -> IngestOutcome.UNCHANGED;
            case LATE_IGNORED -> {
                stats.onSkipped(SkipReason.LATE);
                yield IngestOutcome.LATE_IGNORED;
            }
        };
    }

    @Override
    public void skipUnparseable(String recordKey, String payloadSample) {
        stats.onReceived();
        stats.onSkipped(SkipReason.PARSE);
        if (skipLogLimiter.tryAcquire(clock.millis())) {
            log.warn("Skipping unparseable message: key={} payload={}", recordKey, truncate(payloadSample));
        }
    }

    private IngestOutcome skip(SkipReason reason, String why, RawTagEvent event) {
        stats.onSkipped(reason);
        if (skipLogLimiter.tryAcquire(clock.millis())) {
            log.warn("Skipping message ({}): {} | key={} crane={} tagKey={} value={}",
                    reason.tag(), why, event.recordKey(), event.craneId(), event.tagKey(),
                    truncate(String.valueOf(event.value())));
        }
        return IngestOutcome.SKIPPED;
    }

    private static String truncate(String text) {
        if (text == null || text.length() <= MAX_PAYLOAD_SAMPLE) {
            return text;
        }
        return text.substring(0, MAX_PAYLOAD_SAMPLE) + "...";
    }

    /** 1분 창 안에서 최대 N번만 허용하는 단순 제한기. */
    static final class RateLimiter {
        private final int maxPerMinute;
        private final AtomicLong windowStart = new AtomicLong();
        private final AtomicInteger count = new AtomicInteger();

        RateLimiter(int maxPerMinute) {
            this.maxPerMinute = maxPerMinute;
        }

        boolean tryAcquire(long nowMillis) {
            long start = windowStart.get();
            if (nowMillis - start >= 60_000 && windowStart.compareAndSet(start, nowMillis)) {
                count.set(0);
            }
            return count.incrementAndGet() <= maxPerMinute;
        }
    }
}
