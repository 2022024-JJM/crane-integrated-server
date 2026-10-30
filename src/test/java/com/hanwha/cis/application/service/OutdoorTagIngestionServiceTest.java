package com.hanwha.cis.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.application.port.in.RawTagEvent;
import com.hanwha.cis.application.service.IngestStats.IgnoreReason;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.TagValue;
import com.hanwha.cis.support.InMemoryCraneStateStore;
import com.hanwha.cis.support.LogCapture;
import com.hanwha.cis.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OutdoorTagIngestionServiceTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:47:50.019Z");
    private static final Duration STALE = Duration.ofSeconds(30);
    private static final String DEVICE = "OUTDOOR_PLC";

    private final MutableClock clock = new MutableClock(T0);
    private final CraneStateRegistry registry = new CraneStateRegistry(new InMemoryCraneStateStore());
    private final IngestStats stats = new IngestStats(CraneScope.OUTDOOR);
    private final OutdoorTagIngestionService service = new OutdoorTagIngestionService(registry, stats, clock);
    private final LogCapture logs = LogCapture.of(OutdoorTagIngestionService.class);

    @AfterEach
    void tearDown() {
        logs.close();
    }

    private void send(String tagKey, Object value) {
        send(tagKey, value, T0);
    }

    private void send(String tagKey, Object value, Instant at) {
        service.ingest(new RawTagEvent(DEVICE, "isl-ocean-crane", DEVICE, at.toEpochMilli(), tagKey, value, 0));
    }

    private static String craneIdTag(int slot) {
        return "4_Crane.4_CR%02d.00_Crane_ID_%d".formatted(slot, slot);
    }

    private CraneSnapshot crane(String craneId) {
        return registry.find(craneId).orElseThrow().snapshot(clock.instant(), STALE);
    }

    @Test
    void putsSlotTagsUnderTheCraneOfTheSlotWithOriginalNames() {
        send(craneIdTag(1), 101);
        send("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2000);

        CraneSnapshot gc01 = crane("GC_01");
        assertThat(gc01.scope()).isEqualTo(CraneScope.OUTDOOR);
        assertThat(gc01.bay()).isEqualTo(DEVICE);
        assertThat(gc01.stationId()).isEqualTo("isl-ocean-crane");
        assertThat(gc01.tags()).containsOnlyKeys(craneIdTag(1), "4_Crane.4_CR01.4_POS.03_Center_POS_x1");
        assertThat(gc01.tags().get("4_Crane.4_CR01.4_POS.03_Center_POS_x1").value()).isEqualTo(TagValue.of(2000));
        assertThat(registry.drainDirty()).containsExactly("GC_01");
        assertThat(stats.changed()).isEqualTo(2);
    }

    @Test
    void keepsSlotValuesUntilTheCraneIdArrives() {
        send("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2000);
        assertThat(registry.size()).isZero();
        assertThat(stats.ignored(IgnoreReason.UNASSIGNED)).isEqualTo(1);

        send(craneIdTag(1), 101);

        assertThat(crane("GC_01").tags()).containsOnlyKeys(craneIdTag(1), "4_Crane.4_CR01.4_POS.03_Center_POS_x1");
    }

    @Test
    void passesIrregularSlotNamesThroughUnchanged() {
        send(craneIdTag(23), 3104);
        send("4_Crane.4_CR23.4_POS.CR23_4_POS_04", -1008);

        assertThat(crane("TC_104").tags()).containsKey("4_Crane.4_CR23.4_POS.CR23_4_POS_04");
    }

    @Test
    void movesTagsWithTheSlotWhenTheCraneIdChanges() {
        send(craneIdTag(1), 101);
        send("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2000);

        clock.advance(Duration.ofSeconds(1));
        send(craneIdTag(1), 417, clock.instant());
        send("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2100, clock.instant());

        assertThat(crane("OC_17").tags()).containsOnlyKeys(craneIdTag(1), "4_Crane.4_CR01.4_POS.03_Center_POS_x1");
        assertThat(crane("OC_17").tags().get("4_Crane.4_CR01.4_POS.03_Center_POS_x1").value())
                .isEqualTo(TagValue.of(2100));
        assertThat(crane("GC_01").tags().get("4_Crane.4_CR01.4_POS.03_Center_POS_x1").value())
                .isEqualTo(TagValue.of(2000));

        // 이전 크레인은 갱신이 멈춰 stale 기준이 지나면 offline이 된다.
        clock.advance(STALE.plusSeconds(1));
        send("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2200, clock.instant());
        assertThat(crane("GC_01").online()).isFalse();
        assertThat(crane("OC_17").online()).isTrue();

        // 다른 슬롯에 다시 나타나면 이전 슬롯 이름의 태그는 남지 않는다.
        send(craneIdTag(5), 101, clock.instant());
        assertThat(crane("GC_01").tags()).containsOnlyKeys(craneIdTag(5));
        assertThat(crane("GC_01").online()).isTrue();
    }

    @Test
    void keepsTheFirstSlotWhenTwoSlotsReportTheSameCrane() {
        send(craneIdTag(1), 101);
        send(craneIdTag(2), 101);
        send(craneIdTag(2), 101);
        send("4_Crane.4_CR02.4_POS.03_Center_POS_x2", 9999);

        assertThat(crane("GC_01").tags()).containsOnlyKeys(craneIdTag(1));
        assertThat(stats.ignored(IgnoreReason.UNASSIGNED)).isEqualTo(3);
        assertThat(logs.lines()).singleElement().asString()
                .startsWith("WARN Outdoor slot OUTDOOR_PLC/CR02 reports crane GC_01 that slot OUTDOOR_PLC/CR01");

        send(craneIdTag(1), 0);
        send(craneIdTag(2), 101);

        assertThat(crane("GC_01").tags()).containsOnlyKeys(craneIdTag(2), "4_Crane.4_CR02.4_POS.03_Center_POS_x2");
        assertThat(logs.lines()).last().isEqualTo("INFO Outdoor slot OUTDOOR_PLC/CR02 now has crane GC_01");
    }

    @Test
    void leavesSlotsWithZeroCraneIdUnassigned() {
        send(craneIdTag(20), 0);
        send("4_Crane.4_CR20.4_POS.03_Center_POS_x20", 0);

        assertThat(registry.size()).isZero();
        assertThat(stats.ignored(IgnoreReason.UNASSIGNED)).isEqualTo(2);
    }

    @Test
    void countsCommonBlockWithoutApplyingIt() {
        send("1_Time.01_Year", 2026);
        send("5_Obstacle.OBS01.01_ID", 1);
        send("3_2_Rail_POS.Rail_01_1_x1", 200.5);

        assertThat(registry.size()).isZero();
        assertThat(stats.ignored(IgnoreReason.COMMON)).isEqualTo(3);
        assertThat(stats.received()).isEqualTo(3);
    }

    @Test
    void keepsNullStringAsNullValue() {
        send(craneIdTag(1), 101);
        send("4_Crane.4_CR01.2_slow.52_TL_FW_slow_1", "NULL");

        assertThat(crane("GC_01").tags().get("4_Crane.4_CR01.2_slow.52_TL_FW_slow_1").value())
                .isEqualTo(TagValue.NULL);
        assertThat(stats.nulls()).isEqualTo(1);
    }

    @Test
    void keepsTheNumberOfCraneIdsOutsideTheRuleAndWarnsOnce() {
        send(craneIdTag(3), 601);
        send(craneIdTag(3), 601);

        assertThat(registry.find("ID_601")).isPresent();
        assertThat(logs.lines()).containsExactly(
                "WARN Outdoor Crane_ID 601 does not follow the crane type rule; using crane id ID_601");
    }

    @Test
    void skipsMismatchedOrIncompleteMessages() {
        service.ingest(new RawTagEvent("OTHER", "s", DEVICE, T0.toEpochMilli(), craneIdTag(1), 101, 0));
        service.ingest(new RawTagEvent(DEVICE, "s", DEVICE, null, craneIdTag(1), 101, 0));
        service.ingest(new RawTagEvent(DEVICE, "s", DEVICE, T0.toEpochMilli(), null, 101, 0));
        service.ingest(new RawTagEvent(DEVICE, "s", DEVICE, T0.toEpochMilli(), craneIdTag(1), "abc", 0));
        service.skipUnparseable(DEVICE, "{not json");

        assertThat(stats.skipped(SkipReason.MISMATCH)).isEqualTo(1);
        assertThat(stats.skipped(SkipReason.PARSE)).isEqualTo(4);
        assertThat(stats.received()).isEqualTo(5);
        assertThat(registry.size()).isZero();
    }
}
