package com.hanwha.cis.adapter.in.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.support.LogCapture;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class SummaryLogSchedulerTest {

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final SummaryLogScheduler summary = new SummaryLogScheduler(meters, new ThreadPoolTaskScheduler());
    private final LogCapture logs = LogCapture.of(SummaryLogScheduler.class);

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private AtomicLong counter(String name, String... tags) {
        AtomicLong value = new AtomicLong();
        FunctionCounter.builder(name, value, AtomicLong::get).tags(tags).register(meters);
        return value;
    }

    private void gauge(String name, long value, String... tags) {
        Gauge.builder(name, () -> value).tags(tags).register(meters);
    }

    @Test
    void reportsIndoorAndOutdoorIngestSeparatelyAsDeltas() {
        AtomicLong indoorReceived = counter("cis.ingest.received", "source", "indoor");
        AtomicLong outdoorReceived = counter("cis.ingest.received", "source", "outdoor");
        counter("cis.ingest.skipped", "source", "indoor", "reason", "parse").set(2);
        counter("cis.ingest.nulls", "source", "outdoor").set(28);
        counter("cis.ingest.ignored", "source", "outdoor", "reason", "common").set(255);
        counter("cis.ingest.ignored", "source", "outdoor", "reason", "unassigned").set(627);
        gauge("cis.kafka.partitions", 3, "source", "indoor");
        gauge("cis.kafka.partitions", 1, "source", "outdoor");
        gauge("cis.kafka.connections", 3, "source", "indoor");
        gauge("cis.kafka.connections", 1, "source", "outdoor");
        indoorReceived.set(900);
        outdoorReceived.set(1965);

        summary.tick();
        indoorReceived.addAndGet(100);
        summary.tick();

        assertThat(logs.lines().get(0)).startsWith("INFO ingest indoor received=900 changed=0 skipped=2"
                + " (parse=2 mismatch=0 late=0) null=0 partitions=3"
                + " | outdoor received=1965 changed=0 skipped=0 (parse=0 mismatch=0 late=0) null=28 partitions=1"
                + " common=255 unassigned=627 | kafka connections=4 | history ");
        assertThat(logs.lines().get(1)).startsWith("INFO ingest indoor received=100 changed=0 skipped=0"
                + " (parse=0 mismatch=0 late=0) null=0 partitions=3 | outdoor received=0 ");
    }
}
