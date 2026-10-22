package com.hanwha.cis.support;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/** 로거 하나에 붙어 남긴 로그를 "LEVEL message" 형태로 모은다. try-with-resources로 떼어 낸다. */
public final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    private LogCapture(String name) {
        this.logger = (Logger) LoggerFactory.getLogger(name);
        appender.start();
        logger.addAppender(appender);
    }

    public static LogCapture of(Class<?> type) {
        return new LogCapture(type.getName());
    }

    public static LogCapture of(String name) {
        return new LogCapture(name);
    }

    public List<String> lines() {
        return appender.list.stream().map(e -> e.getLevel() + " " + e.getFormattedMessage()).toList();
    }

    public List<ILoggingEvent> events() {
        return List.copyOf(appender.list);
    }

    public void clear() {
        appender.list.clear();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
    }
}
