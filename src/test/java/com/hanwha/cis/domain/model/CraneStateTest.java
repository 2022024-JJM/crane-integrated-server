package com.hanwha.cis.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CraneStateTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:23:16.993Z");
    private static final Duration STALE = Duration.ofSeconds(30);

    private static TagReading reading(String tag, Object value, Instant at) {
        return new TagReading("isl-ocean-crane", TagKey.parse("F5_1_Bay.C_867." + tag), TagValue.of(value), 0, at);
    }

    @Test
    void firstReadingIsChanged() {
        CraneState state = CraneState.of("C_867");

        assertThat(state.apply(reading("tl_status", false, T0), T0)).isEqualTo(ApplyResult.CHANGED);
        assertThat(state.snapshotAt()).isEqualTo(T0);
    }

    @Test
    void sameValueIsUnchangedButTimestampAdvances() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);

        Instant t1 = T0.plusMillis(100);
        assertThat(state.apply(reading("height", 10.0, t1), t1)).isEqualTo(ApplyResult.UNCHANGED);

        CraneSnapshot snapshot = state.snapshot(t1, STALE);
        assertThat(snapshot.snapshotAt()).isEqualTo(t1);
        assertThat(snapshot.tags().get("height").timestamp()).isEqualTo(t1);
    }

    @Test
    void differentValueIsChanged() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);

        assertThat(state.apply(reading("height", 11, T0.plusMillis(100)), T0.plusMillis(100)))
                .isEqualTo(ApplyResult.CHANGED);
    }

    @Test
    void qualityChangeAloneIsUnchanged() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);

        TagReading worseQuality = new TagReading("isl-ocean-crane", TagKey.parse("F5_1_Bay.C_867.height"),
                TagValue.of(10), 4, T0.plusMillis(100));
        assertThat(state.apply(worseQuality, T0.plusMillis(100))).isEqualTo(ApplyResult.UNCHANGED);
        assertThat(state.snapshot(T0, STALE).tags().get("height").quality()).isEqualTo(4);
    }

    @Test
    void earlierTimestampIsIgnored() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);

        assertThat(state.apply(reading("height", 99, T0.minusMillis(1)), T0.plusMillis(5)))
                .isEqualTo(ApplyResult.LATE_IGNORED);
        assertThat(state.snapshot(T0, STALE).tags().get("height").value()).isEqualTo(TagValue.of(10));
    }

    @Test
    void staleCraneIsOffline() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);

        assertThat(state.snapshot(T0.plus(STALE), STALE).online()).isTrue();
        assertThat(state.snapshot(T0.plus(STALE).plusMillis(1), STALE).online()).isFalse();
        assertThat(state.isOnline(T0.plusSeconds(60), STALE)).isFalse();
    }

    @Test
    void neverReceivedIsOffline() {
        assertThat(CraneState.of("C_867").snapshot(T0, STALE).online()).isFalse();
    }

    @Test
    void fingerprintDependsOnValuesAndOnlineOnly() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("height", 10, T0), T0);
        state.apply(reading("tl_status", true, T0), T0);
        String before = state.snapshot(T0, STALE).fingerprint();

        state.apply(reading("height", 10, T0.plusSeconds(1)), T0.plusSeconds(1));
        assertThat(state.snapshot(T0.plusSeconds(1), STALE).fingerprint()).isEqualTo(before);

        assertThat(state.snapshot(T0.plusSeconds(60), STALE).fingerprint()).isNotEqualTo(before);

        state.apply(reading("height", 11, T0.plusSeconds(2)), T0.plusSeconds(2));
        assertThat(state.snapshot(T0.plusSeconds(2), STALE).fingerprint()).isNotEqualTo(before);
    }

    @Test
    void restoresFromSnapshotWithoutMarkingChange() {
        CraneState original = CraneState.of("C_867");
        original.apply(reading("height", 10, T0), T0);
        CraneSnapshot saved = original.snapshot(T0, STALE);

        CraneState restored = CraneState.fromSnapshot(saved);

        assertThat(restored.snapshot(T0, STALE)).isEqualTo(saved);
        assertThat(restored.apply(reading("height", 10, T0.plusMillis(100)), T0.plusMillis(100)))
                .isEqualTo(ApplyResult.UNCHANGED);
    }

    @Test
    void snapshotSortsTagsByName() {
        CraneState state = CraneState.of("C_867");
        state.apply(reading("z_tag", 1, T0), T0);
        state.apply(reading("a_tag", 2, T0), T0);

        assertThat(state.snapshot(T0, STALE).tags().keySet()).containsExactly("a_tag", "z_tag");
        assertThat(state.snapshot(T0, STALE).values().keySet()).containsExactly("a_tag", "z_tag");
    }
}
