package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.application.support.Durations;
import com.hanwha.cis.application.support.FailureLog;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 전용 스케줄러에서 고정 지연으로 {@link #tick()}을 반복한다. 틱 실패는 로그만 남기고 다음 틱을 계속한다
 * (실패 시작·1분마다 지속·복구만 남김, {@link FailureLog}).
 */
abstract class FixedDelayScheduler implements SmartLifecycle {

    /** HistoryWriter(100)보다 늦게 시작하고 먼저 멈춘다. */
    static final int PHASE = 200;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ThreadPoolTaskScheduler scheduler;
    private final Duration interval;
    private final String task;
    private final FailureLog failures;
    private volatile ScheduledFuture<?> future;

    /** task는 로그에 쓰이는 작업 이름(예: "Snapshot push to Redis"). */
    FixedDelayScheduler(ThreadPoolTaskScheduler scheduler, Duration interval, String task) {
        this.scheduler = scheduler;
        this.interval = interval;
        this.task = task;
        this.failures = new FailureLog(log, task);
    }

    protected abstract void tick();

    @Override
    public void start() {
        if (future != null) {
            return;
        }
        future = scheduler.scheduleWithFixedDelay(this::safeTick, interval);
        log.info("{} scheduled every {}", task, Durations.format(interval));
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
            failures.success();
        } catch (RuntimeException e) {
            failures.failure(e);
        }
    }
}
