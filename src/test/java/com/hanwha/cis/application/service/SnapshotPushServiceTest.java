package com.hanwha.cis.application.service;

import static com.hanwha.cis.support.Readings.reading;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import com.hanwha.cis.support.RecordingSnapshotPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class SnapshotPushServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:23:16.993Z");
    private static final Duration STALE = Duration.ofSeconds(30);

    private final MutableClock clock = new MutableClock(T0);
    private final InMemoryCraneStateStore store = new InMemoryCraneStateStore();
    private final CraneStateRegistry registry = new CraneStateRegistry(store);
    private final RecordingSnapshotPublisher publisher = new RecordingSnapshotPublisher();
    private final SnapshotPushService service = new SnapshotPushService(registry, store, publisher, clock, STALE);

    @Test
    void pushesOnlyDirtyCranes() {
        registry.apply(reading("C_867", "height", 10, T0), T0);
        registry.apply(reading("C_871", "height", 20, T0), T0);

        assertThat(service.pushDirty()).isEqualTo(2);
        assertThat(store.findAll()).extracting(CraneSnapshot::craneId).containsExactlyInAnyOrder("C_867", "C_871");
        assertThat(publisher.published()).hasSize(1);

        assertThat(service.pushDirty()).isZero();

        registry.apply(reading("C_871", "height", 21, T0.plusSeconds(1)), T0.plusSeconds(1));
        assertThat(service.pushDirty()).isEqualTo(1);
        assertThat(publisher.published().getLast()).extracting(CraneSnapshot::craneId).containsExactly("C_871");
    }

    @Test
    void unchangedValueDoesNotTriggerPush() {
        registry.apply(reading("C_867", "height", 10, T0), T0);
        service.pushDirty();

        registry.apply(reading("C_867", "height", 10, T0.plusSeconds(1)), T0.plusSeconds(1));

        assertThat(service.pushDirty()).isZero();
    }

    @Test
    void pushesOnlineTransitionsWithoutValueChange() {
        registry.apply(reading("C_867", "height", 10, T0), T0);
        service.pushDirty();
        assertThat(store.find("C_867").orElseThrow().online()).isTrue();

        clock.advance(STALE.plusSeconds(1));
        assertThat(service.pushDirty()).isEqualTo(1);
        assertThat(store.find("C_867").orElseThrow().online()).isFalse();
        assertThat(service.pushDirty()).isZero();

        registry.apply(reading("C_867", "height", 10, clock.instant()), clock.instant());
        assertThat(service.pushDirty()).isEqualTo(1);
        assertThat(store.find("C_867").orElseThrow().online()).isTrue();
    }

    @Test
    void logsOnlineTransitionsOncePerTick() {
        try (LogCapture logs = LogCapture.of(SnapshotPushService.class)) {
            registry.apply(reading("C_867", "height", 10, T0), T0);
            registry.apply(reading("C_871", "height", 20, T0), T0);
            service.pushDirty();
            service.pushDirty();

            clock.advance(STALE.plusSeconds(1));
            service.pushDirty();
            clock.advance(Duration.ofSeconds(90));
            registry.apply(reading("C_867", "height", 11, clock.instant()), clock.instant());
            service.pushDirty();

            assertThat(logs.lines()).containsExactly(
                    "INFO Cranes online (2/2 online): C_867, C_871",
                    "WARN Cranes offline, no data for 30s (0/2 online): C_867, C_871",
                    "INFO Cranes online (1/2 online): C_867 (offline 1m 30s)");
        }
    }

    @Test
    void failedPushIsRetriedOnNextTick() {
        AtomicBoolean fail = new AtomicBoolean(true);
        RecordingSnapshotPublisher flaky = new RecordingSnapshotPublisher(snapshots -> {
            if (fail.get()) {
                throw new IllegalStateException("redis down");
            }
        });
        SnapshotPushService flakyService = new SnapshotPushService(registry, store, flaky, clock, STALE);
        registry.apply(reading("C_867", "height", 10, T0), T0);

        assertThatIllegalStateException().isThrownBy(flakyService::pushDirty);

        fail.set(false);
        assertThat(flakyService.pushDirty()).isEqualTo(1);
        assertThat(flaky.published()).hasSize(2);
        assertThat(flaky.published().getLast()).extracting(CraneSnapshot::craneId).isEqualTo(List.of("C_867"));
        assertThat(flakyService.published()).as("failed publishes are not counted").isEqualTo(1);
    }
}
