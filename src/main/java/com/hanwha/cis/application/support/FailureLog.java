package com.hanwha.cis.application.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * 같은 대상이 연속으로 실패할 때 로그를 시작·지속·복구 세 종류로 줄인다.
 * 시작은 WARN과 원인(가장 안쪽 예외까지), 지속 중에는 1분마다 WARN 한 줄, 복구되면 INFO로 지속 시간과 실패 횟수를 남긴다.
 * 스택트레이스는 10분에 한 번만 붙인다. 여러 스레드에서 불러도 된다.
 */
public final class FailureLog {

    static final Duration REMIND_EVERY = Duration.ofSeconds(60);
    static final Duration STACK_TRACE_EVERY = Duration.ofMinutes(10);
    private static final int MAX_CAUSES = 4;

    private final Logger log;
    private final String target;
    private final Clock clock;
    private final Supplier<String> detail;

    private volatile boolean failing;
    private Instant failingSince;
    private long failures;
    private Instant lastLoggedAt;
    private Instant lastStackTraceAt;

    public FailureLog(Logger log, String target) {
        this(log, target, Clock.systemUTC(), () -> null);
    }

    /** detail은 로그를 남길 때마다 불려 줄 끝에 [ ]로 붙는다(null이면 생략). */
    public FailureLog(Logger log, String target, Clock clock, Supplier<String> detail) {
        this.log = log;
        this.target = target;
        this.clock = clock;
        this.detail = detail;
    }

    public synchronized void failure(Throwable e) {
        Instant now = clock.instant();
        if (!failing) {
            failing = true;
            failingSince = now;
            failures = 1;
            lastLoggedAt = now;
            if (lastStackTraceAt == null || !now.isBefore(lastStackTraceAt.plus(STACK_TRACE_EVERY))) {
                lastStackTraceAt = now;
                log.warn("{} failed: {}{}", target, describe(e), detail(), e);
            } else {
                log.warn("{} failed: {}{}", target, describe(e), detail());
            }
            return;
        }
        failures++;
        if (!now.isBefore(lastLoggedAt.plus(REMIND_EVERY))) {
            lastLoggedAt = now;
            log.warn("{} still failing for {} ({} failures): {}{}", target,
                    Durations.format(Duration.between(failingSince, now)), failures, describe(e), detail());
        }
    }

    public void success() {
        if (!failing) {
            return;
        }
        synchronized (this) {
            if (!failing) {
                return;
            }
            failing = false;
            log.info("{} recovered after {} ({} failures){}", target,
                    Durations.format(Duration.between(failingSince, clock.instant())), failures, detail());
        }
    }

    public boolean isFailing() {
        return failing;
    }

    private String detail() {
        String text = detail.get();
        return text == null ? "" : " [" + text + "]";
    }

    /** 예외와 원인 체인을 한 줄로: {@code RedisConnectionFailureException: Unable to connect <- ConnectException: Connection refused}. */
    public static String describe(Throwable e) {
        StringBuilder text = new StringBuilder();
        String previousMessage = null;
        Throwable current = e;
        for (int depth = 0; current != null && depth < MAX_CAUSES; depth++) {
            String message = current.getMessage();
            if (depth > 0) {
                text.append(" <- ");
            }
            text.append(current.getClass().getSimpleName());
            if (message != null && !message.equals(previousMessage)) {
                text.append(": ").append(message);
            }
            previousMessage = message;
            current = current.getCause() == current ? null : current.getCause();
        }
        return text.toString();
    }
}
