package com.hanwha.cis.config;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RepeatedLogFilterTest {

    private final LoggerContext context = new LoggerContext();
    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private final AtomicLong now = new AtomicLong(1_000_000);

    @BeforeEach
    void setUp() {
        RepeatedLogFilter filter = new RepeatedLogFilter();
        filter.setContext(context);
        filter.addLoggerPrefix("org.apache.kafka");
        filter.setClock(now::get);
        filter.start();
        context.addTurboFilter(filter);

        events.setContext(context);
        events.start();
        Logger root = context.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        root.addAppender(events);
    }

    @Test
    void passesOneRepeatPerWindowAcrossConsumersAndReportsSuppressedCount() {
        Logger kafka = context.getLogger("org.apache.kafka.clients.NetworkClient");
        for (int i = 0; i < 50; i++) {
            kafka.warn("[Consumer clientId=ingest-indoor-" + i % 3 + ", groupId=cis] Connection to node {} ({}) could not"
                    + " be established. Node may not be available.", -1, "localhost/127.0.0.1:29092");
        }
        assertThat(lines()).hasSize(1);

        now.addAndGet(59_999);
        kafka.warn("[Consumer clientId=ingest-indoor-1, groupId=cis] Connection to node {} ({}) could not be established."
                + " Node may not be available.", -1, "localhost/127.0.0.1:29092");
        assertThat(lines()).hasSize(1);

        now.addAndGet(1);
        kafka.warn("[Consumer clientId=ingest-indoor-2, groupId=cis] Connection to node {} ({}) could not be established."
                + " Node may not be available.", -1, "localhost/127.0.0.1:29092");
        assertThat(lines()).containsExactly(
                "WARN org.apache.kafka.clients.NetworkClient [Consumer clientId=ingest-indoor-0, groupId=cis] Connection to"
                        + " node -1 (localhost/127.0.0.1:29092) could not be established. Node may not be available.",
                "WARN com.hanwha.cis.config.RepeatedLogFilter org.apache.kafka.clients.NetworkClient: suppressed 50"
                        + " repeat(s) of the following message in the last 1m 0s",
                "WARN org.apache.kafka.clients.NetworkClient [Consumer clientId=ingest-indoor-2, groupId=cis] Connection to"
                        + " node -1 (localhost/127.0.0.1:29092) could not be established. Node may not be available.");
    }

    @Test
    void treatsDifferentMessagesAndLevelsSeparately() {
        Logger kafka = context.getLogger("org.apache.kafka.clients.NetworkClient");
        kafka.warn("Bootstrap broker {} disconnected", "localhost:29092");
        kafka.warn("Bootstrap broker {} disconnected", "localhost:29092");
        kafka.warn("The metadata response from the cluster reported a recoverable issue with correlation id 17 : {}",
                "{topic=UNKNOWN_TOPIC_OR_PARTITION}");
        kafka.warn("The metadata response from the cluster reported a recoverable issue with correlation id 18 : {}",
                "{topic=UNKNOWN_TOPIC_OR_PARTITION}");
        kafka.error("Bootstrap broker {} disconnected", "localhost:29092");

        assertThat(lines()).hasSize(3);
    }

    @Test
    void leavesOtherLoggersAndInfoAlone() {
        Logger own = context.getLogger("com.hanwha.cis.adapter.out.persistence.HistoryWriter");
        Logger kafka = context.getLogger("org.springframework.kafka.listener.KafkaMessageListenerContainer");
        for (int i = 0; i < 3; i++) {
            own.warn("History write queue is full");
            kafka.info("crane-integrated-server: partitions assigned: [ocean.crane.indoor.event-" + i + "]");
            kafka.debug("Received: 0 records");
        }

        assertThat(lines()).hasSize(6);
    }

    private List<String> lines() {
        return events.list.stream()
                .map(e -> e.getLevel() + " " + e.getLoggerName() + " " + e.getFormattedMessage())
                .toList();
    }
}
