package com.hanwha.cis.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TagKeyTest {

    @Test
    void parsesThreeSegments() {
        TagKey key = TagKey.parse("F5_1_Bay.C_867.tl_status");

        assertThat(key.bay()).isEqualTo("F5_1_Bay");
        assertThat(key.craneId()).isEqualTo("C_867");
        assertThat(key.tagName()).isEqualTo("tl_status");
        assertThat(key.raw()).isEqualTo("F5_1_Bay.C_867.tl_status");
    }

    @ParameterizedTest
    @ValueSource(strings = {"C_867.tl_status", "F5.C_867.tl.status", "F5..tl_status", ".C_867.tl_status", "F5.C_867.", ""})
    void rejectsWrongSegmentCountOrBlankSegment(String raw) {
        assertThatIllegalArgumentException().isThrownBy(() -> TagKey.parse(raw));
    }

    @Test
    void rejectsNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> TagKey.parse(null));
    }
}
