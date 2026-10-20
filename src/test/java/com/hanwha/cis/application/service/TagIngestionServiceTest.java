package com.hanwha.cis.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.application.port.in.IngestTagReadingUseCase.IngestOutcome;
import com.hanwha.cis.application.port.in.IngestTagReadingUseCase.RawTagEvent;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import com.hanwha.cis.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TagIngestionServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:23:16.993Z");

    private final MutableClock clock = new MutableClock(T0);
    private final CraneStateRegistry registry = new CraneStateRegistry(new InMemoryCraneStateStore());
    private final IngestStats stats = new IngestStats();
    private final TagIngestionService service = new TagIngestionService(registry, stats, clock);

    private static RawTagEvent event(String key, String craneId, String tagKey, Object value, long at) {
        return new RawTagEvent(key, "isl-ocean-crane", craneId, at, tagKey, value, 0);
    }

    @Test
    void appliesValidEventAndCounts() {
        RawTagEvent e = event("C_867", "C_867", "F5_1_Bay.C_867.tl_status", false, T0.toEpochMilli());

        assertThat(service.ingest(e)).isEqualTo(IngestOutcome.CHANGED);
        assertThat(service.ingest(event("C_867", "C_867", "F5_1_Bay.C_867.tl_status", false,
                T0.toEpochMilli() + 100))).isEqualTo(IngestOutcome.UNCHANGED);

        assertThat(stats.received()).isEqualTo(2);
        assertThat(stats.changed()).isEqualTo(1);
        assertThat(stats.skippedTotal()).isZero();
        assertThat(registry.find("C_867")).isPresent();
        assertThat(registry.drainDirty()).containsExactly("C_867");
        assertThat(registry.find("C_867").orElseThrow().snapshot(T0, Duration.ofSeconds(30))
                .lastReceivedAt()).isEqualTo(T0);
    }

    @Test
    void skipsCraneIdMismatchBetweenTagKeyAndEnvelope() {
        assertThat(service.ingest(event("C_867", "C_867", "F5_1_Bay.C_871.tl_status", false, 1L)))
                .isEqualTo(IngestOutcome.SKIPPED);
        assertThat(stats.skipped(SkipReason.MISMATCH)).isEqualTo(1);
        assertThat(registry.size()).isZero();
    }

    @Test
    void skipsRecordKeyMismatch() {
        assertThat(service.ingest(event("C_871", "C_867", "F5_1_Bay.C_867.tl_status", false, 1L)))
                .isEqualTo(IngestOutcome.SKIPPED);
        assertThat(stats.skipped(SkipReason.MISMATCH)).isEqualTo(1);
    }

    @Test
    void allowsMissingRecordKey() {
        assertThat(service.ingest(event(null, "C_867", "F5_1_Bay.C_867.tl_status", true, 1L)))
                .isEqualTo(IngestOutcome.CHANGED);
    }

    @Test
    void skipsUnparseableTagKeyValueOrTimestamp() {
        assertThat(service.ingest(event("C_867", "C_867", "C_867.tl_status", false, 1L)))
                .isEqualTo(IngestOutcome.SKIPPED);
        assertThat(service.ingest(event("C_867", "C_867", "F5_1_Bay.C_867.tl_status", "abc", 1L)))
                .isEqualTo(IngestOutcome.SKIPPED);
        assertThat(service.ingest(new RawTagEvent("C_867", "s", "C_867", null, "F5_1_Bay.C_867.tl_status", 1, 0)))
                .isEqualTo(IngestOutcome.SKIPPED);
        assertThat(stats.skipped(SkipReason.PARSE)).isEqualTo(3);
        assertThat(stats.received()).isEqualTo(3);
    }

    @Test
    void countsLateValues() {
        service.ingest(event("C_867", "C_867", "F5_1_Bay.C_867.height", 10, T0.toEpochMilli()));

        assertThat(service.ingest(event("C_867", "C_867", "F5_1_Bay.C_867.height", 99, T0.toEpochMilli() - 1)))
                .isEqualTo(IngestOutcome.LATE_IGNORED);
        assertThat(stats.skipped(SkipReason.LATE)).isEqualTo(1);
    }

    @Test
    void countsUnparseableMessagesReportedByAdapter() {
        service.skipUnparseable("C_867", "{not json");

        assertThat(stats.received()).isEqualTo(1);
        assertThat(stats.skipped(SkipReason.PARSE)).isEqualTo(1);
    }

    @Test
    void rateLimiterAllowsBurstThenBlocksUntilNextWindow() {
        TagIngestionService.RateLimiter limiter = new TagIngestionService.RateLimiter(2);

        assertThat(limiter.tryAcquire(60_000)).isTrue();
        assertThat(limiter.tryAcquire(60_001)).isTrue();
        assertThat(limiter.tryAcquire(60_002)).isFalse();
        assertThat(limiter.tryAcquire(120_000)).isTrue();
    }
}
