package com.hanwha.cis.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SkipLogLimiterTest {

    @Test
    void allowsBurstThenBlocksUntilNextWindow() {
        SkipLogLimiter limiter = new SkipLogLimiter(2);

        assertThat(limiter.tryAcquire(60_000)).isTrue();
        assertThat(limiter.tryAcquire(60_001)).isTrue();
        assertThat(limiter.tryAcquire(60_002)).isFalse();
        assertThat(limiter.tryAcquire(120_000)).isTrue();
    }
}
