package com.hanwha.cis.application.service;

import static com.hanwha.cis.support.IndoorReadings.reading;
import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.application.port.out.CraneHistoryPort.DiffRow;
import com.hanwha.cis.application.port.out.CraneHistoryPort.HistoryRow;
import com.hanwha.cis.support.InMemoryCraneHistoryPort;
import com.hanwha.cis.support.InMemoryCraneRegistryPort;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import com.hanwha.cis.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HistoryRecordingServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:23:16.993Z");
    private static final Duration STALE = Duration.ofSeconds(30);

    private final MutableClock clock = new MutableClock(T0);
    private final CraneStateRegistry registry = new CraneStateRegistry(new InMemoryCraneStateStore());
    private final InMemoryCraneHistoryPort history = new InMemoryCraneHistoryPort();
    private final InMemoryCraneRegistryPort cranes = new InMemoryCraneRegistryPort();
    private final HistoryRecordingService service =
            new HistoryRecordingService(registry, history, cranes, clock, STALE);

    @BeforeEach
    void firstReading() {
        registry.apply(reading("C_867", "height", 10, T0), T0);
    }

    @Test
    void writesHistoryAndDiffRowOnFirstTick() {
        service.recordTick();

        assertThat(history.historyRows()).extracting(HistoryRow::time).containsExactly(T0);
        assertThat(history.diffRows()).extracting(DiffRow::time, DiffRow::online)
                .containsExactly(org.assertj.core.groups.Tuple.tuple(T0, true));
        assertThat(cranes.upserts()).hasSize(1);
        assertThat(cranes.upserts().getFirst().craneId()).isEqualTo("C_867");
        assertThat(cranes.upserts().getFirst().bay()).isEqualTo("F5_1_Bay");
    }

    @Test
    void writesNothingWhenNothingArrived() {
        service.recordTick();
        clock.advance(Duration.ofSeconds(1));

        service.recordTick();

        assertThat(history.historyRows()).hasSize(1);
        assertThat(history.diffRows()).hasSize(1);
    }

    @Test
    void writesHistoryRowWhenSnapshotAdvancesButNoDiffWhenValuesSame() {
        service.recordTick();
        clock.advance(Duration.ofSeconds(1));
        registry.apply(reading("C_867", "height", 10.0, T0.plusSeconds(1)), clock.instant());

        service.recordTick();

        assertThat(history.historyRows()).extracting(HistoryRow::time).containsExactly(T0, T0.plusSeconds(1));
        assertThat(history.diffRows()).hasSize(1);
    }

    @Test
    void writesDiffRowWhenValueChanges() {
        service.recordTick();
        clock.advance(Duration.ofSeconds(2));
        registry.apply(reading("C_867", "height", 11, T0.plusSeconds(2)), clock.instant());

        service.recordTick();

        assertThat(history.diffRows()).extracting(DiffRow::time).containsExactly(T0, T0.plusSeconds(2));
        assertThat(history.diffRows().getLast().snapshot().values().get("height").raw()).isEqualTo(11.0);
    }

    @Test
    void goingOfflineWritesDiffAtDetectionTimeButNoHistoryRow() {
        service.recordTick();
        clock.advance(STALE.plusSeconds(1));

        service.recordTick();

        assertThat(history.historyRows()).hasSize(1);
        assertThat(history.diffRows()).hasSize(2);
        DiffRow offline = history.diffRows().getLast();
        assertThat(offline.online()).isFalse();
        assertThat(offline.time()).isEqualTo(clock.instant());
        assertThat(offline.snapshot().snapshotAt()).isEqualTo(T0);
    }

    @Test
    void comingBackOnlineWritesDiffAtSnapshotTime() {
        service.recordTick();
        clock.advance(STALE.plusSeconds(1));
        service.recordTick();
        clock.advance(Duration.ofSeconds(10));
        Instant back = clock.instant();
        registry.apply(reading("C_867", "height", 10, back), back);

        service.recordTick();

        assertThat(history.historyRows()).extracting(HistoryRow::time).containsExactly(T0, back);
        assertThat(history.diffRows()).hasSize(3);
        assertThat(history.diffRows().getLast().online()).isTrue();
        assertThat(history.diffRows().getLast().time()).isEqualTo(back);
    }

    @Test
    void refreshesLastSeenOnlyAfterInterval() {
        service.recordTick();
        clock.advance(Duration.ofSeconds(30));
        registry.apply(reading("C_867", "height", 10, clock.instant()), clock.instant());
        service.recordTick();
        assertThat(cranes.upserts()).hasSize(1);

        clock.advance(Duration.ofSeconds(31));
        registry.apply(reading("C_867", "height", 10, clock.instant()), clock.instant());
        service.recordTick();

        assertThat(cranes.upserts()).hasSize(2);
        assertThat(cranes.upserts().getLast().seenAt()).isEqualTo(clock.instant());
    }
}
