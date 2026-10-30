package com.hanwha.cis.application.service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** 건너뛴 메시지 로그를 1분 창 안에서 최대 N번만 허용하는 단순 제한기. 내업·외업 수신 서비스가 같이 쓴다. */
final class SkipLogLimiter {

    private final int maxPerMinute;
    private final AtomicLong windowStart = new AtomicLong();
    private final AtomicInteger count = new AtomicInteger();

    SkipLogLimiter(int maxPerMinute) {
        this.maxPerMinute = maxPerMinute;
    }

    boolean tryAcquire(long nowMillis) {
        long start = windowStart.get();
        if (nowMillis - start >= 60_000 && windowStart.compareAndSet(start, nowMillis)) {
            count.set(0);
        }
        return count.incrementAndGet() <= maxPerMinute;
    }
}
