package com.hanwha.cis.adapter.in.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.adapter.in.scheduler.IngestWatchScheduler.Status;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class IngestWatchSchedulerTest {

    private static final Duration STEP = IngestWatchScheduler.INTERVAL;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-23T00:00:00Z"));
    private final Feed indoor = new Feed(3);
    private final Feed outdoor = new Feed(1);
    private final IngestWatchScheduler watch;
    private final LogCapture logs = LogCapture.of(IngestWatchScheduler.class);

    /** 토픽 하나의 미터 값(수신 수, 연결 수, 파티션 수). */
    private static final class Feed {
        final AtomicLong received = new AtomicLong();
        final AtomicInteger connections;
        final AtomicInteger partitions;

        Feed(int partitions) {
            this.connections = new AtomicInteger(partitions);
            this.partitions = new AtomicInteger(partitions);
        }

        void register(SimpleMeterRegistry meters, CraneScope scope) {
            FunctionCounter.builder("cis.ingest.received", received, AtomicLong::get)
                    .tag("source", scope.id()).register(meters);
            Gauge.builder("cis.kafka.connections", connections, AtomicInteger::get)
                    .tag("source", scope.id()).register(meters);
            Gauge.builder("cis.kafka.partitions", partitions, AtomicInteger::get)
                    .tag("source", scope.id()).register(meters);
        }
    }

    IngestWatchSchedulerTest() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        indoor.register(meters, CraneScope.INDOOR);
        outdoor.register(meters, CraneScope.OUTDOOR);
        Map<CraneScope, String> topics = new LinkedHashMap<>();
        topics.put(CraneScope.INDOOR, "ocean.crane.indoor.event");
        topics.put(CraneScope.OUTDOOR, "ocean.crane.outdoor.event");
        watch = new IngestWatchScheduler(meters, clock, Duration.ofSeconds(30), "localhost:29092", topics,
                new ThreadPoolTaskScheduler());
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void tellsWhichPartOfThePipelineStopped() {
        receiveFor(Duration.ofSeconds(10), indoor, outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.RECEIVING);

        indoor.connections.set(0);
        indoor.partitions.set(0);
        silenceFor(Duration.ofSeconds(30), outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.BROKER_UNREACHABLE);

        indoor.connections.set(1);
        tickAfter(STEP, outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.NO_PARTITIONS);

        indoor.partitions.set(3);
        tickAfter(STEP, outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.NO_MESSAGES);

        tickAfter(STEP, indoor, outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.RECEIVING);
        assertThat(watch.status(CraneScope.OUTDOOR)).isEqualTo(Status.RECEIVING);

        assertThat(logs.lines()).filteredOn(line -> line.contains("'ocean.crane.indoor.event'")).hasSize(5);
        assertThat(logs.lines()).hasSize(6);
        assertThat(logs.lines().get(0)).isEqualTo(
                "INFO Kafka ingest started: receiving from topic 'ocean.crane.indoor.event' (partitions=3)");
        assertThat(logs.lines().get(1)).isEqualTo(
                "INFO Kafka ingest started: receiving from topic 'ocean.crane.outdoor.event' (partitions=1)");
        assertThat(logs.lines().get(2)).startsWith("WARN Kafka ingest stopped: no connection to broker"
                + " localhost:29092 for topic 'ocean.crane.indoor.event', no messages for 30s.");
        assertThat(logs.lines().get(3)).contains("no partition of topic 'ocean.crane.indoor.event' is assigned");
        assertThat(logs.lines().get(4)).contains("Upstream is not publishing");
        assertThat(logs.lines().get(5)).startsWith("INFO Kafka ingest resumed after 45s");
    }

    @Test
    void reportsAMissingOutdoorTopicWhileIndoorKeepsReceiving() {
        outdoor.connections.set(1);
        outdoor.partitions.set(0);
        receiveFor(Duration.ofSeconds(35), indoor);

        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.RECEIVING);
        assertThat(watch.status(CraneScope.OUTDOOR)).isEqualTo(Status.NO_PARTITIONS);
        assertThat(logs.lines()).last().asString().startsWith("WARN Kafka ingest stopped: connected to localhost:29092"
                + " but no partition of topic 'ocean.crane.outdoor.event' is assigned");
    }

    @Test
    void remindsOncePerMinuteWhileStopped() {
        receiveFor(Duration.ofSeconds(10), indoor, outdoor);
        silenceFor(Duration.ofMinutes(3), outdoor);

        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.NO_MESSAGES);
        assertThat(logs.lines()).filteredOn(line -> line.startsWith("WARN Kafka ingest still stopped"))
                .containsExactly(
                        "WARN Kafka ingest still stopped for 1m 30s (topic 'ocean.crane.indoor.event'): no messages"
                                + " from upstream (connections=3, partitions=3)",
                        "WARN Kafka ingest still stopped for 2m 30s (topic 'ocean.crane.indoor.event'): no messages"
                                + " from upstream (connections=3, partitions=3)");
    }

    @Test
    void reportsWhenNothingArrivesAfterStartup() {
        indoor.connections.set(0);
        indoor.partitions.set(0);
        silenceFor(Duration.ofSeconds(30), outdoor); // 첫 틱(5s)부터 감시하므로 30s 시점에는 25s째
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.WAITING);

        tickAfter(STEP, outdoor);
        assertThat(watch.status(CraneScope.INDOOR)).isEqualTo(Status.BROKER_UNREACHABLE);
        assertThat(logs.lines()).filteredOn(line -> line.startsWith("WARN")).singleElement().asString()
                .startsWith("WARN Kafka ingest stopped");
    }

    /** 주어진 토픽들만 STEP마다 받으면서 duration 동안 틱한다. */
    private void receiveFor(Duration duration, Feed... receiving) {
        for (Duration elapsed = Duration.ZERO; elapsed.compareTo(duration) <= 0; elapsed = elapsed.plus(STEP)) {
            tickAfter(elapsed.isZero() ? Duration.ZERO : STEP, receiving);
        }
    }

    /** 주어진 토픽들만 받고 나머지는 조용한 채로 duration 동안 틱한다. */
    private void silenceFor(Duration duration, Feed... receiving) {
        for (Duration elapsed = STEP; elapsed.compareTo(duration) <= 0; elapsed = elapsed.plus(STEP)) {
            tickAfter(STEP, receiving);
        }
    }

    private void tickAfter(Duration step, Feed... receiving) {
        for (Feed feed : receiving) {
            feed.received.addAndGet(100);
        }
        clock.advance(step);
        watch.tick();
    }
}
