package com.hanwha.cis.adapter.out.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.adapter.out.persistence.DroppedWritesLog.Reason;
import com.hanwha.cis.adapter.out.persistence.HistoryWriter.CraneUpsert;
import com.hanwha.cis.adapter.out.persistence.HistoryWriter.DiffInsert;
import com.hanwha.cis.adapter.out.persistence.HistoryWriter.HistoryInsert;
import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class DroppedWritesLogTest {

    private static final Instant T0 = Instant.parse("2026-10-23T05:00:00Z");

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-23T06:00:00Z"));
    private final LogCapture logs = LogCapture.of(DroppedWritesLogTest.class);
    private final DroppedWritesLog dropped = new DroppedWritesLog(LoggerFactory.getLogger(DroppedWritesLogTest.class),
            clock, () -> "queue=10000/10000");

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void reportsFirstDropRightAwayThenOncePerMinuteWithTimeRanges() {
        dropped.record(Reason.WRITE_FAILED, List.of(history(0), diff(1), history(55)));
        dropped.report();

        clock.advance(Duration.ofSeconds(30));
        dropped.record(Reason.WRITE_FAILED, List.of(history(56), history(80)));
        dropped.record(Reason.QUEUE_FULL, List.of(history(1080)));
        dropped.report();
        clock.advance(Duration.ofSeconds(30));
        dropped.record(Reason.QUEUE_FULL, List.of(history(1140), new CraneUpsert("C_867", "s", "b", at(1141))));
        dropped.report();

        assertThat(logs.lines()).containsExactly(
                "ERROR History writes dropped: 3 failed to write (time 2026-10-23T05:00:00Z ~ 2026-10-23T05:00:55Z)"
                        + " [queue=10000/10000]",
                "ERROR History writes dropped: 2 failed to write (time 2026-10-23T05:00:56Z ~ 2026-10-23T05:01:20Z),"
                        + " 3 rejected by full queue (time 2026-10-23T05:18:00Z ~ 2026-10-23T05:19:01Z)"
                        + " [queue=10000/10000]");
    }

    @Test
    void reportsTheEndAfterAQuietMinute() {
        dropped.record(Reason.WRITE_FAILED, List.of(history(0)));
        dropped.report();
        clock.advance(Duration.ofMinutes(2));
        dropped.record(Reason.WRITE_FAILED, List.of(history(10), history(20)));
        dropped.report();

        clock.advance(Duration.ofSeconds(59));
        dropped.report();
        assertThat(logs.lines()).hasSize(2);

        clock.advance(Duration.ofSeconds(1));
        dropped.report();
        assertThat(logs.lines()).last()
                .isEqualTo("INFO History writes no longer dropped: 3 write(s) dropped in total over 2m 0s");

        logs.clear();
        dropped.record(Reason.QUEUE_FULL, List.of(history(30)));
        dropped.report();
        assertThat(logs.lines()).as("a new episode reports its first drop right away").singleElement().asString()
                .startsWith("ERROR History writes dropped: 1 rejected by full queue (time 2026-10-23T05:00:30Z)");
    }

    @Test
    void flushReportsWhatIsLeftRightAway() {
        dropped.record(Reason.WRITE_FAILED, List.of(history(0)));
        dropped.report();
        dropped.record(Reason.SHUTDOWN, List.of(history(5), history(6)));
        dropped.report();
        assertThat(logs.lines()).hasSize(1);

        dropped.flush();

        assertThat(logs.lines()).last().isEqualTo("ERROR History writes dropped: 2 unwritten at shutdown"
                + " (time 2026-10-23T05:00:05Z ~ 2026-10-23T05:00:06Z) [queue=10000/10000]");
    }

    private static Instant at(int seconds) {
        return T0.plusSeconds(seconds);
    }

    private static HistoryInsert history(int seconds) {
        return new HistoryInsert(at(seconds), "C_867", "F5_1_Bay", "{}");
    }

    private static DiffInsert diff(int seconds) {
        return new DiffInsert(at(seconds), "C_867", "F5_1_Bay", true, "{}");
    }
}
