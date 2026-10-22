package com.hanwha.cis.application.support;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class FailureLogTest {

    private static final String LOGGER = "test.FailureLogTest";

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-23T00:00:00Z"));
    private final LogCapture logs = LogCapture.of(LOGGER);
    private final FailureLog failures = new FailureLog(LoggerFactory.getLogger(LOGGER), "Snapshot push to Redis",
            clock, () -> "queue=1");
    private final RuntimeException error = new IllegalStateException("Unable to connect to Redis",
            new ConnectException("Connection refused: localhost/127.0.0.1:6379"));

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void logsStartReminderAndRecoveryOnly() {
        failures.failure(error);
        for (int i = 0; i < 10; i++) {
            clock.advance(Duration.ofSeconds(1));
            failures.failure(error);
        }
        assertThat(logs.lines()).containsExactly("WARN Snapshot push to Redis failed: IllegalStateException: Unable"
                + " to connect to Redis <- ConnectException: Connection refused: localhost/127.0.0.1:6379 [queue=1]");

        clock.advance(Duration.ofSeconds(60));
        failures.failure(error);
        clock.advance(Duration.ofSeconds(5));
        failures.success();
        failures.success();

        assertThat(logs.lines()).hasSize(3);
        assertThat(logs.lines().get(1)).startsWith("WARN Snapshot push to Redis still failing for 1m 10s"
                + " (12 failures): IllegalStateException");
        assertThat(logs.lines().get(2))
                .isEqualTo("INFO Snapshot push to Redis recovered after 1m 15s (12 failures) [queue=1]");
        assertThat(failures.isFailing()).isFalse();
    }

    @Test
    void attachesStackTraceAtMostEveryTenMinutes() {
        failures.failure(error);
        failures.success();
        clock.advance(Duration.ofMinutes(1));
        failures.failure(error);
        failures.success();
        clock.advance(Duration.ofMinutes(10));
        failures.failure(error);

        assertThat(logs.events()).filteredOn(e -> e.getLevel().toString().equals("WARN"))
                .extracting(e -> e.getThrowableProxy() != null)
                .containsExactly(true, false, true);
    }

    @Test
    void describesCauseChainWithoutRepeatingMessages() {
        RuntimeException wrapped = new RuntimeException("Connection refused",
                new ConnectException("Connection refused"));

        assertThat(FailureLog.describe(wrapped)).isEqualTo("RuntimeException: Connection refused <- ConnectException");
        assertThat(FailureLog.describe(new NullPointerException())).isEqualTo("NullPointerException");
    }

    @Test
    void formatsDurationsForHumans() {
        assertThat(Durations.format(Duration.ofMillis(250))).isEqualTo("250ms");
        assertThat(Durations.format(Duration.ofSeconds(45))).isEqualTo("45s");
        assertThat(Durations.format(Duration.ofSeconds(192))).isEqualTo("3m 12s");
        assertThat(Durations.format(Duration.ofMinutes(125))).isEqualTo("2h 5m");
        assertThat(Durations.format(Duration.ofHours(50))).isEqualTo("2d 2h");
    }
}
