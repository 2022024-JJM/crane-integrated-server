package com.hanwha.cis.adapter.in.scheduler;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 전용 스케줄러에서 고정 지연으로 {@link #tick()}을 반복한다. 틱 실패는 로그만 남기고 다음 틱을 계속한다
 * (같은 오류는 10초에 한 번만 WARN).
 */
abstract class FixedDelayScheduler implements SmartLifecycle {

    /** HistoryWriter(100)보다 늦게 시작하고 먼저 멈춘다. */
    static final int PHASE = 200;
    private static final long WARN_INTERVAL_MILLIS = 10_000;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ThreadPoolTaskScheduler scheduler;
    private final Duration interval;
    private volatile ScheduledFuture<?> future;
    private volatile long lastWarnAt;

    FixedDelayScheduler(ThreadPoolTaskScheduler scheduler, Duration interval) {
        this.scheduler = scheduler;
        this.interval = interval;
    }

    protected abstract void tick();

    @Override
    public void start() {
        if (future != null) {
            return;
        }
        future = scheduler.scheduleWithFixedDelay(this::safeTick, interval);
        log.info("{} started (every {})", getClass().getSimpleName(), interval);
    }

    @Override
    public void stop() {
        ScheduledFuture<?> f = future;
        if (f != null) {
            f.cancel(false);
            future = null;
        }
    }

    @Override
    public boolean isRunning() {
        return future != null;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private void safeTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            long now = System.currentTimeMillis();
            if (now - lastWarnAt >= WARN_INTERVAL_MILLIS) {
                lastWarnAt = now;
                log.warn("{} tick failed: {}", getClass().getSimpleName(), e.toString());
            } else {
                log.debug("{} tick failed: {}", getClass().getSimpleName(), e.toString());
            }
        }
    }
}
