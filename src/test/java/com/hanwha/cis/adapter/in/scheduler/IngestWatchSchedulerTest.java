package com.hanwha.cis.adapter.in.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.adapter.in.scheduler.IngestWatchScheduler.Status;
import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class IngestWatchSchedulerTest {

    private static final Duration STEP = IngestWatchScheduler.INTERVAL;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-23T00:00:00Z"));
    private final AtomicLong received = new AtomicLong();
    private final AtomicInteger connections = new AtomicInteger(3);
    private final AtomicInteger partitions = new AtomicInteger(3);
    private final IngestWatchScheduler watch;
    private final LogCapture logs = LogCapture.of(IngestWatchScheduler.class);

    IngestWatchSchedulerTest() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        FunctionCounter.builder("cis.ingest.received", received, AtomicLong::get).register(meters);
        Gauge.builder("cis.kafka.connections", connections, AtomicInteger::get).register(meters);
        Gauge.builder("cis.kafka.partitions", partitions, AtomicInteger::get).register(meters);
        watch = new IngestWatchScheduler(meters, clock, Duration.ofSeconds(30), "localhost:29092",
                "ocean.crane.indoor.event", new ThreadPoolTaskScheduler());
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void tellsWhichPartOfThePipelineStopped() {
        receiveFor(Duration.ofSeconds(10));
        assertThat(watch.status()).isEqualTo(Status.RECEIVING);

        connections.set(0);
        partitions.set(0);
        silenceFor(Duration.ofSeconds(30));
        assertThat(watch.status()).isEqualTo(Status.BROKER_UNREACHABLE);

        connections.set(1);
        tickAfter(STEP);
        assertThat(watch.status()).isEqualTo(Status.NO_PARTITIONS);

        partitions.set(3);
        tickAfter(STEP);
        assertThat(watch.status()).isEqualTo(Status.NO_MESSAGES);

        received.incrementAndGet();
        tickAfter(STEP);
        assertThat(watch.status()).isEqualTo(Status.RECEIVING);

        assertThat(logs.lines()).hasSize(5);
        assertThat(logs.lines().get(0)).startsWith("INFO Kafka ingest started");
        assertThat(logs.lines().get(1)).startsWith("WARN Kafka ingest stopped: no connection to broker"
                + " localhost:29092, no messages for 30s.");
        assertThat(logs.lines().get(2)).contains("no partition of topic 'ocean.crane.indoor.event' is assigned");
        assertThat(logs.lines().get(3)).contains("Upstream is not publishing");
        assertThat(logs.lines().get(4)).startsWith("INFO Kafka ingest resumed after 45s");
    }

    @Test
    void remindsOncePerMinuteWhileStopped() {
        receiveFor(Duration.ofSeconds(10));
        silenceFor(Duration.ofMinutes(3));

        assertThat(watch.status()).isEqualTo(Status.NO_MESSAGES);
        assertThat(logs.lines()).filteredOn(line -> line.startsWith("WARN Kafka ingest still stopped"))
                .containsExactly(
                        "WARN Kafka ingest still stopped for 1m 30s: no messages from upstream (connections=3,"
                                + " partitions=3)",
                        "WARN Kafka ingest still stopped for 2m 30s: no messages from upstream (connections=3,"
                                + " partitions=3)");
    }

    @Test
    void reportsWhenNothingArrivesAfterStartup() {
        connections.set(0);
        partitions.set(0);
        silenceFor(Duration.ofSeconds(30)); // 첫 틱(5s)부터 감시하므로 30s 시점에는 25s째
        assertThat(watch.status()).isEqualTo(Status.WAITING);

        tickAfter(STEP);
        assertThat(watch.status()).isEqualTo(Status.BROKER_UNREACHABLE);
        assertThat(logs.lines()).singleElement().asString().startsWith("WARN Kafka ingest stopped");
    }

    private void receiveFor(Duration duration) {
        for (Duration elapsed = Duration.ZERO; elapsed.compareTo(duration) <= 0; elapsed = elapsed.plus(STEP)) {
            received.addAndGet(100);
            tickAfter(elapsed.isZero() ? Duration.ZERO : STEP);
        }
    }

    private void silenceFor(Duration duration) {
        for (Duration elapsed = STEP; elapsed.compareTo(duration) <= 0; elapsed = elapsed.plus(STEP)) {
            tickAfter(STEP);
        }
    }

    private void tickAfter(Duration step) {
        clock.advance(step);
        watch.tick();
    }
}
