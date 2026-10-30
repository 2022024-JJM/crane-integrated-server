package com.hanwha.cis.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.TagState;
import com.hanwha.cis.domain.model.TagValue;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

/** Redis·이력에 저장한 스냅샷 JSON을 다시 읽는 경로. Boot JsonMapper 등록은 REST·E2E 테스트가 확인한다. */
class DomainJacksonComponentTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:47:50.019Z");

    private final JsonMapper json = JsonMapper.builder().addModule(new SimpleModule()
            .addSerializer(TagValue.class, new DomainJacksonComponent.TagValueSerializer())
            .addDeserializer(TagValue.class, new DomainJacksonComponent.TagValueDeserializer())
            .addSerializer(CraneScope.class, new DomainJacksonComponent.CraneScopeSerializer())
            .addDeserializer(CraneScope.class, new DomainJacksonComponent.CraneScopeDeserializer()))
            .build();

    @Test
    void writesScopeAndNullValueAndReadsThemBack() {
        CraneSnapshot snapshot = new CraneSnapshot("GC_01", CraneScope.OUTDOOR, "isl-ocean-crane", "OUTDOOR_PLC",
                T0, T0, true, Map.of(
                        "4_Crane.4_CR01.2_slow.52_TL_FW_slow_1", new TagState(TagValue.NULL, 0, T0),
                        "4_Crane.4_CR01.4_POS.03_Center_POS_x1", new TagState(TagValue.of(2000), 0, T0)));

        String text = json.writeValueAsString(snapshot);

        assertThat(text).contains("\"scope\":\"outdoor\"")
                .contains("\"4_Crane.4_CR01.2_slow.52_TL_FW_slow_1\":{\"value\":null,")
                .contains("\"4_Crane.4_CR01.4_POS.03_Center_POS_x1\":{\"value\":2000,");
        assertThat(json.readValue(text, CraneSnapshot.class)).isEqualTo(snapshot);
    }

    @Test
    void readsStoredSnapshotWithoutScopeAsIndoor() {
        String stored = "{\"craneId\":\"C_867\",\"stationId\":\"isl-ocean-crane\",\"bay\":\"F5_1_Bay\","
                + "\"snapshotAt\":\"2026-10-30T04:47:50.019Z\",\"lastReceivedAt\":\"2026-10-30T04:47:50.019Z\","
                + "\"online\":true,\"tags\":{\"height\":{\"value\":10,\"quality\":0,"
                + "\"timestamp\":\"2026-10-30T04:47:50.019Z\"}}}";

        CraneSnapshot snapshot = json.readValue(stored, CraneSnapshot.class);

        assertThat(snapshot.scope()).isEqualTo(CraneScope.INDOOR);
        assertThat(snapshot.tags().get("height").value()).isEqualTo(TagValue.of(10));
    }
}
