package com.hanwha.cis.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class TagValueTest {

    @Test
    void normalizesNumbersToDouble() {
        assertThat(TagValue.of(1)).isEqualTo(new TagValue.NumberValue(1.0));
        assertThat(TagValue.of(1L)).isEqualTo(TagValue.of(1.0));
        assertThat(TagValue.of("1")).isEqualTo(TagValue.of(1));
        assertThat(TagValue.of(" 12.5 ")).isEqualTo(new TagValue.NumberValue(12.5));
    }

    @Test
    void normalizesBooleans() {
        assertThat(TagValue.of(true)).isEqualTo(new TagValue.BoolValue(true));
        assertThat(TagValue.of("true")).isEqualTo(TagValue.of(Boolean.TRUE));
        assertThat(TagValue.of("FALSE")).isEqualTo(new TagValue.BoolValue(false));
    }

    @Test
    void booleanAndNumberAreNotEqual() {
        assertThat(TagValue.of(true)).isNotEqualTo(TagValue.of(1));
        assertThat(TagValue.of(false)).isNotEqualTo(TagValue.of(0));
    }

    @Test
    void passesThroughExistingTagValue() {
        TagValue value = TagValue.of(3);
        assertThat(TagValue.of(value)).isSameAs(value);
    }

    @Test
    void reportsIntegralNumbers() {
        assertThat(((TagValue.NumberValue) TagValue.of(12)).isIntegral()).isTrue();
        assertThat(((TagValue.NumberValue) TagValue.of(12.5)).isIntegral()).isFalse();
    }

    @Test
    void rejectsUnsupportedValues() {
        assertThatIllegalArgumentException().isThrownBy(() -> TagValue.of(null));
        assertThatIllegalArgumentException().isThrownBy(() -> TagValue.of("abc"));
        assertThatIllegalArgumentException().isThrownBy(() -> TagValue.of("NaN"));
        assertThatIllegalArgumentException().isThrownBy(() -> TagValue.of(new Object()));
    }
}
