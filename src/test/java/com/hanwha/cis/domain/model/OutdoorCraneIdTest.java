package com.hanwha.cis.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutdoorCraneIdTest {

    @Test
    void followsCraneTypeRule() {
        assertThat(OutdoorCraneId.of(101)).contains("GC_01");
        assertThat(OutdoorCraneId.of(207)).contains("TTC_07");
        assertThat(OutdoorCraneId.of(348)).contains("TC_48");
        assertThat(OutdoorCraneId.of(3104)).contains("TC_104");
        assertThat(OutdoorCraneId.of(3100)).contains("TC_100");
        assertThat(OutdoorCraneId.of(417)).contains("OC_17");
        assertThat(OutdoorCraneId.of(449)).contains("OC_49");
        assertThat(OutdoorCraneId.of(450)).contains("CC_50");
        assertThat(OutdoorCraneId.of(452)).contains("CC_52");
        assertThat(OutdoorCraneId.of(4120)).contains("OC_120");
        assertThat(OutdoorCraneId.of(4512)).contains("CC_512");
        assertThat(OutdoorCraneId.of(501)).contains("LLC_01");
    }

    @Test
    void zeroOrNegativeIsUnassigned() {
        assertThat(OutdoorCraneId.of(0)).isEmpty();
        assertThat(OutdoorCraneId.of(-1)).isEmpty();
    }

    @Test
    void numbersOutsideTheRuleKeepTheirNumber() {
        assertThat(OutdoorCraneId.of(99)).contains("ID_99");
        assertThat(OutdoorCraneId.of(601)).contains("ID_601");
        assertThat(OutdoorCraneId.of(1101)).contains("ID_1101");
        assertThat(OutdoorCraneId.of(10001)).contains("ID_10001");
        assertThat(OutdoorCraneId.isKnown(601)).isFalse();
        assertThat(OutdoorCraneId.isKnown(101)).isTrue();
    }
}
