package com.hanwha.cis.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.hanwha.cis.application.support.Durations;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.slf4j.Marker;

/**
 * 외부 라이브러리의 같은 WARN·ERROR 로그가 반복되면 windowSeconds(기본 60초)에 한 번만 통과시킨다.
 * Kafka 재접속 실패처럼 초당 수십 줄씩 같은 문장이 찍히는 것을 막기 위함이다. 다시 통과할 때 그 사이 생략한 건수를 한 줄 앞에 남긴다.
 * 대상 로거와 창 길이는 logback-spring.xml에서 정한다. 같은 문장 판정은 형식 문자열 기준이며,
 * 앞의 [...] 접두어(Kafka clientId 등)와 숫자는 무시해 컨슈머가 여러 개여도 한 문장으로 본다.
 * INFO 이하는 건드리지 않는다(파티션 번호만 다른 할당 로그처럼 숫자를 무시하면 정보가 사라지는 경우가 있어서).
 */
public class RepeatedLogFilter extends TurboFilter {

    private static final Pattern BRACKET_PREFIX = Pattern.compile("^\\[[^]]*]\\s*");
    private static final Pattern DIGITS = Pattern.compile("\\d+");
    private static final int MAX_KEYS = 1_000;

    private final List<String> loggerPrefixes = new ArrayList<>();
    private int windowSeconds = 60;
    private LongSupplier clock = System::currentTimeMillis;
    private RepeatWindows windows;
    private Logger notice;

    public void addLoggerPrefix(String prefix) {
        loggerPrefixes.add(prefix.trim());
    }

    public void setWindowSeconds(int windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    void setClock(LongSupplier clock) {
        this.clock = clock;
    }

    @Override
    public void start() {
        if (loggerPrefixes.isEmpty()) {
            addError("RepeatedLogFilter needs at least one loggerPrefix");
            return;
        }
        windows = new RepeatWindows(windowSeconds * 1_000L, MAX_KEYS);
        notice = ((LoggerContext) getContext()).getLogger(RepeatedLogFilter.class);
        super.start();
    }

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] params,
            Throwable t) {
        // isXxxEnabled() 호출(format == null), INFO 이하, 어차피 버려질 레벨은 건드리지 않는다.
        if (format == null || level == null || !level.isGreaterOrEqual(Level.WARN)
                || !level.isGreaterOrEqual(logger.getEffectiveLevel()) || !isTarget(logger.getName())) {
            return FilterReply.NEUTRAL;
        }
        long now = clock.getAsLong();
        RepeatWindows.Pass pass = windows.tryPass(key(logger, level, format), now);
        if (pass == null) {
            return FilterReply.DENY;
        }
        if (pass.suppressed() > 0) {
            report(level, logger.getName(), pass.suppressed(),
                    Durations.format(Duration.ofMillis(now - pass.previousPassAt())));
        }
        return FilterReply.NEUTRAL;
    }

    private boolean isTarget(String loggerName) {
        for (String prefix : loggerPrefixes) {
            if (loggerName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static String key(Logger logger, Level level, String format) {
        String body = DIGITS.matcher(BRACKET_PREFIX.matcher(format).replaceFirst("")).replaceAll("#");
        return logger.getName() + '|' + level + '|' + body;
    }

    private void report(Level level, String loggerName, long suppressed, String elapsed) {
        String message = "{}: suppressed {} repeat(s) of the following message in the last {}";
        if (level.isGreaterOrEqual(Level.ERROR)) {
            notice.error(message, loggerName, suppressed, elapsed);
        } else {
            notice.warn(message, loggerName, suppressed, elapsed);
        }
    }
}
