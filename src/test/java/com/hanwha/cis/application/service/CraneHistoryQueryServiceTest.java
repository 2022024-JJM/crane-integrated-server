package com.hanwha.cis.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.application.port.in.InvalidQueryException;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.Change;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.Frame;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryChanges;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryFrames;
import com.hanwha.cis.application.service.CraneHistoryQueryService.Limits;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.TagState;
import com.hanwha.cis.domain.model.TagValue;
import com.hanwha.cis.support.InMemoryCraneHistoryPort;
import com.hanwha.cis.support.InMemoryCraneRegistryPort;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CraneHistoryQueryServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:00:00Z");
    private static final Duration SEC = Duration.ofSeconds(1);
    private static final Limits LIMITS = new Limits(Duration.ofHours(1), SEC, 100, 2, 10);

    private final InMemoryCraneHistoryPort history = new InMemoryCraneHistoryPort();
    private final InMemoryCraneRegistryPort cranes = new InMemoryCraneRegistryPort();
    private final CraneStateRegistry registry = new CraneStateRegistry(new InMemoryCraneStateStore());
    private final CraneHistoryQueryService service =
            new CraneHistoryQueryService(history, cranes, registry, LIMITS);

    private static CraneSnapshot snapshot(Instant at, boolean online, int height) {
        return new CraneSnapshot("C_867", "isl-ocean-crane", "F5_1_Bay", at, at, online,
                Map.of("height", new TagState(TagValue.of(height), 0, at)));
    }

    @BeforeEach
    void knownCrane() {
        cranes.upsertSeen("C_867", "isl-ocean-crane", "F5_1_Bay", T0);
    }

    @Test
    void appliesBaselineAndSamplesAndHolds() {
        history.addHistory(T0.minusSeconds(10), snapshot(T0.minusSeconds(10), true, 0));
        history.addHistory(T0.plusMillis(500), snapshot(T0.plusMillis(500), true, 1));
        history.addHistory(T0.plusMillis(2500), snapshot(T0.plusMillis(2500), true, 2));
        history.addHistory(T0.plusMillis(4500), snapshot(T0.plusMillis(4500), true, 3));

        HistoryFrames frames = service.frames("C_867", T0, T0.plusSeconds(5), null);

        assertThat(frames.step()).isEqualTo(SEC);
        assertThat(frames.tags()).containsExactly("height");
        assertThat(frames.baseline().time()).isEqualTo(T0.minusSeconds(10));
        assertThat(frames.baseline().values().get("height")).isEqualTo(TagValue.of(0));
        assertThat(frames.truncated()).isFalse();
        assertThat(frames.frames()).extracting(Frame::time).containsExactly(
                T0, T0.plusSeconds(1), T0.plusSeconds(2), T0.plusSeconds(3), T0.plusSeconds(4), T0.plusSeconds(5));
        assertThat(frames.frames()).extracting(f -> f.values().get("height").raw())
                .containsExactly(0.0, 1.0, 1.0, 2.0, 2.0, 3.0);
    }

    @Test
    void rowExactlyAtFromIsTheBaseline() {
        history.addHistory(T0, snapshot(T0, true, 7));

        HistoryFrames frames = service.frames("C_867", T0, T0.plusSeconds(2), SEC);

        assertThat(frames.baseline().time()).isEqualTo(T0);
        assertThat(frames.frames()).allSatisfy(f -> assertThat(f.values().get("height").raw()).isEqualTo(7.0));
    }

    @Test
    void framesWithoutBaselineAreEmptyUntilFirstRow() {
        history.addHistory(T0.plusMillis(1500), snapshot(T0.plusMillis(1500), true, 5));

        HistoryFrames frames = service.frames("C_867", T0, T0.plusSeconds(2), SEC);

        assertThat(frames.baseline()).isNull();
        assertThat(frames.frames().get(0).values()).isEmpty();
        assertThat(frames.frames().get(1).values()).isEmpty();
        assertThat(frames.frames().get(2).values().get("height")).isEqualTo(TagValue.of(5));
    }

    @Test
    void truncatesWhenRowLimitExceeded() {
        Limits tight = new Limits(Duration.ofHours(1), SEC, 3, 2, 10);
        CraneHistoryQueryService tightService = new CraneHistoryQueryService(history, cranes, registry, tight);
        for (int i = 1; i <= 4; i++) {
            history.addHistory(T0.plusMillis(i * 200L), snapshot(T0.plusMillis(i * 200L), true, i));
        }

        HistoryFrames frames = tightService.frames("C_867", T0, T0.plusSeconds(2), SEC);

        assertThat(frames.truncated()).isTrue();
        assertThat(frames.frames()).extracting(Frame::time).containsExactly(T0);
    }

    @Test
    void rejectsInvalidRanges() {
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", T0, T0, SEC));
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", T0.plusSeconds(1), T0, SEC));
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", T0, T0.plus(Duration.ofHours(1)).plusMillis(1), SEC));
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", null, T0, SEC));
    }

    @Test
    void rejectsStepBelowOneSecondAndTooManyPoints() {
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", T0, T0.plusSeconds(10), Duration.ofMillis(500)));
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.frames("C_867", T0, T0.plusSeconds(100), SEC))
                .withMessageContaining("too many points");
    }

    @Test
    void rejectsUnknownCrane() {
        assertThatExceptionOfType(CraneNotFoundException.class)
                .isThrownBy(() -> service.frames("C_999", T0, T0.plusSeconds(1), SEC));
        assertThatExceptionOfType(CraneNotFoundException.class)
                .isThrownBy(() -> service.changes("C_999", T0, T0.plusSeconds(1), null, null));
    }

    @Test
    void pagesChangesWithCursor() {
        for (int i = 0; i < 4; i++) {
            history.addDiff(T0.plusSeconds(i), snapshot(T0.plusSeconds(i), i != 2, i));
        }

        HistoryChanges first = service.changes("C_867", T0, T0.plusSeconds(10), null, null);
        assertThat(first.items()).extracting(Change::time).containsExactly(T0, T0.plusSeconds(1));
        assertThat(first.nextCursor()).isEqualTo(T0.plusSeconds(1));

        HistoryChanges second = service.changes("C_867", T0, T0.plusSeconds(10), first.nextCursor(), null);
        assertThat(second.items()).extracting(Change::time, Change::online).containsExactly(
                org.assertj.core.groups.Tuple.tuple(T0.plusSeconds(2), false),
                org.assertj.core.groups.Tuple.tuple(T0.plusSeconds(3), true));
        assertThat(second.nextCursor()).isNull();
        assertThat(second.items().getFirst().values().get("height")).isEqualTo(TagValue.of(2));
    }

    @Test
    void rejectsChangeLimitOutOfBounds() {
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.changes("C_867", T0, T0.plusSeconds(10), null, 0));
        assertThatExceptionOfType(InvalidQueryException.class)
                .isThrownBy(() -> service.changes("C_867", T0, T0.plusSeconds(10), null, 11));
    }
}
