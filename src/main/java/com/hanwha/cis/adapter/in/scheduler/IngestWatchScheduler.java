package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.application.support.Durations;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.config.SchedulingConfig;
import com.hanwha.cis.domain.model.CraneScope;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * 5초마다 Kafka 수신 상태를 토픽(indoor·outdoor)별로 판정하고, 상태가 바뀔 때만 로그를 남긴다(문제가 이어지면 1분마다 한 줄).
 * 마지막 수신 후 cis.state.stale-threshold(기본 30s)가 지나면 원인을 나눈다:
 * 브로커 연결 없음(Kafka 자체) / 연결됐지만 파티션 할당 없음(토픽·광고 주소) / 둘 다 정상인데 메시지 없음(상류 발행 중단).
 * 한 토픽이 멈춰도 다른 토픽이 받고 있으면 그대로 드러나도록 따로 본다.
 * 값은 Micrometer(cis.ingest.received, cis.kafka.connections, cis.kafka.partitions, 태그 source)에서 읽는다.
 */
@Component
public class IngestWatchScheduler extends FixedDelayScheduler {

    enum Status {
        WAITING(false), RECEIVING(false), BROKER_UNREACHABLE(true), NO_PARTITIONS(true), NO_MESSAGES(true);

        private final boolean problem;

        Status(boolean problem) {
            this.problem = problem;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(IngestWatchScheduler.class);
    static final Duration INTERVAL = Duration.ofSeconds(5);
    static final Duration REMIND_EVERY = Duration.ofSeconds(60);

    private final MeterRegistry meters;
    private final Clock clock;
    private final Duration silenceThreshold;
    private final String bootstrap;
    private final Map<CraneScope, Feed> feeds = new LinkedHashMap<>();

    @Autowired
    public IngestWatchScheduler(MeterRegistry meters, Clock clock, CisProperties properties, KafkaProperties kafka,
            @Qualifier(SchedulingConfig.WATCH_SCHEDULER) ThreadPoolTaskScheduler scheduler) {
        this(meters, clock, properties.state().staleThreshold(), String.join(",", kafka.getBootstrapServers()),
                topics(properties), scheduler);
    }

    IngestWatchScheduler(MeterRegistry meters, Clock clock, Duration silenceThreshold, String bootstrap,
            Map<CraneScope, String> topics, ThreadPoolTaskScheduler scheduler) {
        super(scheduler, INTERVAL, "Kafka ingest watch");
        this.meters = meters;
        this.clock = clock;
        this.silenceThreshold = silenceThreshold;
        this.bootstrap = bootstrap;
        topics.forEach((scope, topic) -> feeds.put(scope, new Feed(scope.id(), topic)));
    }

    private static Map<CraneScope, String> topics(CisProperties properties) {
        Map<CraneScope, String> topics = new LinkedHashMap<>();
        topics.put(CraneScope.INDOOR, properties.kafka().topic());
        topics.put(CraneScope.OUTDOOR, properties.kafka().outdoorTopic());
        return topics;
    }

    Status status(CraneScope scope) {
        return feeds.get(scope).status;
    }

    @Override
    protected void tick() {
        Instant now = clock.instant();
        for (Feed feed : feeds.values()) {
            feed.tick(now);
        }
    }

    /** 토픽 하나의 수신 상태. */
    private final class Feed {

        private final String source;
        private final String topic;

        private Instant watchingSince;
        private double lastReceived;
        private Instant lastReceivedAt;
        private Status status = Status.WAITING;
        private Instant problemSince;
        private Instant lastLoggedAt;

        Feed(String source, String topic) {
            this.source = source;
            this.topic = topic;
        }

        void tick(Instant now) {
            if (watchingSince == null) {
                watchingSince = now;
            }
            double received = counter("cis.ingest.received");
            if (received > lastReceived) {
                lastReceived = received;
                lastReceivedAt = now;
            }
            Status next = judge(now);
            if (next != status) {
                onChange(next, now);
                status = next;
            } else if (next.problem && !now.isBefore(lastLoggedAt.plus(REMIND_EVERY))) {
                lastLoggedAt = now;
                log.warn("Kafka ingest still stopped for {} (topic '{}'): {} (connections={}, partitions={})",
                        Durations.format(Duration.between(problemSince, now)), topic, shortReason(next),
                        gauge("cis.kafka.connections"), gauge("cis.kafka.partitions"));
            }
        }

        private Status judge(Instant now) {
            if (Duration.between(silentSince(), now).compareTo(silenceThreshold) < 0) {
                return lastReceivedAt == null ? Status.WAITING : Status.RECEIVING;
            }
            if (gauge("cis.kafka.connections") == 0) {
                return Status.BROKER_UNREACHABLE;
            }
            if (gauge("cis.kafka.partitions") == 0) {
                return Status.NO_PARTITIONS;
            }
            return Status.NO_MESSAGES;
        }

        private void onChange(Status next, Instant now) {
            lastLoggedAt = now;
            if (!next.problem) {
                if (status.problem) {
                    log.info("Kafka ingest resumed after {} (topic '{}', partitions={})",
                            Durations.format(Duration.between(problemSince, now)), topic,
                            gauge("cis.kafka.partitions"));
                } else if (next == Status.RECEIVING) {
                    log.info("Kafka ingest started: receiving from topic '{}' (partitions={})", topic,
                            gauge("cis.kafka.partitions"));
                }
                return;
            }
            if (!status.problem) {
                problemSince = silentSince();
            }
            String silent = Durations.format(Duration.between(silentSince(), now));
            switch (next) {
                case BROKER_UNREACHABLE -> log.warn("Kafka ingest stopped: no connection to broker {} for topic '{}',"
                        + " no messages for {}. Check that the ISL Kafka container is running and reachable at that"
                        + " address.", bootstrap, topic, silent);
                case NO_PARTITIONS -> log.warn("Kafka ingest stopped: connected to {} but no partition of topic '{}'"
                        + " is assigned, no messages for {}. Check that the topic exists (ISL Kafka Provider) and that"
                        + " the broker's advertised address is reachable from this server.", bootstrap, topic, silent);
                case NO_MESSAGES -> log.warn("Kafka ingest stopped: connected with {} partition(s) of topic '{}'"
                        + " assigned but no messages for {}. Upstream is not publishing: check ISL Kafka Provider /"
                        + " MQTT Agent and the Node-RED simulator.", gauge("cis.kafka.partitions"), topic, silent);
                default -> throw new IllegalStateException("not a problem status: " + next);
            }
        }

        private String shortReason(Status problem) {
            return switch (problem) {
                case BROKER_UNREACHABLE -> "broker " + bootstrap + " unreachable";
                case NO_PARTITIONS -> "no partition assigned";
                case NO_MESSAGES -> "no messages from upstream";
                default -> problem.name();
            };
        }

        private Instant silentSince() {
            return lastReceivedAt != null ? lastReceivedAt : watchingSince;
        }

        private double counter(String name) {
            return meters.find(name).tag("source", source).functionCounters().stream()
                    .mapToDouble(FunctionCounter::count).sum();
        }

        /** 게이지가 없거나 읽기에 실패하면(NaN) -1. 판정에서 0(없음)으로 오인하지 않게 한다. */
        private long gauge(String name) {
            Gauge gauge = meters.find(name).tag("source", source).gauge();
            double value = gauge == null ? Double.NaN : gauge.value();
            return Double.isNaN(value) ? -1 : Math.round(value);
        }
    }
}
