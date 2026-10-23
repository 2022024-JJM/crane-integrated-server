package com.hanwha.cis.adapter.in.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class PushWatchSchedulerTest {

    private static final Duration STEP = PushWatchScheduler.INTERVAL;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-23T00:00:00Z"));
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong forwarded = new AtomicLong();
    private final PushWatchScheduler watch;
    private final LogCapture logs = LogCapture.of(PushWatchScheduler.class);

    PushWatchSchedulerTest() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        FunctionCounter.builder("cis.push.published", published, AtomicLong::get).register(meters);
        FunctionCounter.builder("cis.push.forwarded", forwarded, AtomicLong::get).register(meters);
        watch = new PushWatchScheduler(meters, clock, "cis:crane:snapshots", new ThreadPoolTaskScheduler());
    }

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void staysQuietWhileMessagesComeBack() {
        for (int i = 0; i < 30; i++) {
            published.addAndGet(20);
            forwarded.addAndGet(20);
            tickAfter(STEP);
        }

        assertThat(watch.stalled()).isFalse();
        assertThat(logs.lines()).isEmpty();
    }

    @Test
    void staysQuietWhenNothingIsPublished() {
        publishAndForwardFor(Duration.ofSeconds(10));
        for (int i = 0; i < 30; i++) {
            tickAfter(STEP);
        }

        assertThat(watch.stalled()).isFalse();
        assertThat(logs.lines()).isEmpty();
    }

    @Test
    void warnsWhenPublishedMessagesStopComingBackAndWhenTheyResume() {
        publishAndForwardFor(Duration.ofSeconds(10));

        publishOnlyFor(Duration.ofSeconds(30)); // 첫 틱(15s)에 처음 보므로 40s 시점에는 25s째
        assertThat(watch.stalled()).as("below the threshold").isFalse();
        publishOnlyFor(STEP);
        assertThat(watch.stalled()).isTrue();

        publishOnlyFor(Duration.ofMinutes(1));
        forwarded.addAndGet(20);
        tickAfter(STEP);
        assertThat(watch.stalled()).isFalse();

        assertThat(logs.lines()).containsExactly(
                "WARN Live push stalled: 140 message(s) published to Redis channel 'cis:crane:snapshots' for 30s"
                        + " but none came back through the subscription, so STOMP clients get no updates. Check the"
                        + " Redis subscription and the redis-sub- threads.",
                "WARN Live push still stalled for 1m 30s (380 message(s) not delivered)",
                "INFO Live push resumed after 1m 35s");
    }

    private void publishAndForwardFor(Duration duration) {
        for (Duration elapsed = Duration.ZERO; elapsed.compareTo(duration) < 0; elapsed = elapsed.plus(STEP)) {
            published.addAndGet(20);
            forwarded.addAndGet(20);
            tickAfter(STEP);
        }
    }

    private void publishOnlyFor(Duration duration) {
        for (Duration elapsed = Duration.ZERO; elapsed.compareTo(duration) < 0; elapsed = elapsed.plus(STEP)) {
            published.addAndGet(20);
            tickAfter(STEP);
        }
    }

    private void tickAfter(Duration step) {
        clock.advance(step);
        watch.tick();
    }
}
