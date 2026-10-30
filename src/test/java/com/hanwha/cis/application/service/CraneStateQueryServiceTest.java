package com.hanwha.cis.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.TagKey;
import com.hanwha.cis.domain.model.TagReading;
import com.hanwha.cis.domain.model.TagValue;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import com.hanwha.cis.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class CraneStateQueryServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:47:50.019Z");
    private static final Duration STALE = Duration.ofSeconds(30);

    private final MutableClock clock = new MutableClock(T0);
    private final InMemoryCraneStateStore store = new InMemoryCraneStateStore();
    private final CraneStateRegistry registry = new CraneStateRegistry(store);
    private final CraneStateQueryService service = new CraneStateQueryService(registry, store, clock, STALE);

    private void receive(CraneScope scope, String bay, String craneId) {
        registry.apply(new TagReading(scope, "isl-ocean-crane", new TagKey(bay, craneId, "tag"), TagValue.of(1), 0,
                T0), T0);
    }

    private List<CraneSnapshot> all() {
        return registry.all().stream().map(state -> state.snapshot(T0, STALE)).toList();
    }

    @Test
    void separatesScopesInStoreAndInMemory() {
        receive(CraneScope.INDOOR, "F5_1_Bay", "C_867");
        receive(CraneScope.OUTDOOR, "OUTDOOR_PLC", "GC_01");

        assertThat(service.list(CraneScope.INDOOR)).extracting(CraneSnapshot::craneId).containsExactly("C_867");
        assertThat(service.list(CraneScope.OUTDOOR)).extracting(CraneSnapshot::craneId).containsExactly("GC_01");

        store.save(all());
        assertThat(service.list(CraneScope.INDOOR)).extracting(CraneSnapshot::craneId).containsExactly("C_867");
        assertThat(service.list(CraneScope.OUTDOOR)).extracting(CraneSnapshot::craneId).containsExactly("GC_01");
    }

    @Test
    void doesNotReturnACraneOfAnotherScope() {
        receive(CraneScope.OUTDOOR, "OUTDOOR_PLC", "GC_01");

        assertThat(service.get(CraneScope.OUTDOOR, "GC_01").craneId()).isEqualTo("GC_01");
        assertThatExceptionOfType(CraneNotFoundException.class)
                .isThrownBy(() -> service.get(CraneScope.INDOOR, "GC_01"));

        store.save(all());
        assertThatExceptionOfType(CraneNotFoundException.class)
                .isThrownBy(() -> service.get(CraneScope.INDOOR, "GC_01"));
    }
}
