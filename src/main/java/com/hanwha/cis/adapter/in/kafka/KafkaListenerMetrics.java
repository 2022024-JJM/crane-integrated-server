package com.hanwha.cis.adapter.in.kafka;

import com.hanwha.cis.domain.model.CraneScope;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * 리스너 컨슈머들의 브로커 연결 수와 할당 파티션 수를 게이지 cis.kafka.connections / cis.kafka.partitions 로 노출한다.
 * 태그 source=indoor|outdoor 로 리스너를 나눈다(수신 카운터 cis.ingest.* 와 같은 태그).
 * 수신 감시(IngestWatchScheduler)와 요약 로그가 읽는다. 컨슈머 metrics()는 잠금 없이 읽혀 다른 스레드에서 불러도 된다.
 */
@Component
public class KafkaListenerMetrics implements MeterBinder {

    private static final Map<CraneScope, String> LISTENERS = Map.of(
            CraneScope.INDOOR, IndoorCraneEventKafkaListener.LISTENER_ID,
            CraneScope.OUTDOOR, OutdoorCraneEventKafkaListener.LISTENER_ID);

    private final KafkaListenerEndpointRegistry registry;

    public KafkaListenerMetrics(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void bindTo(MeterRegistry meters) {
        LISTENERS.forEach((scope, listenerId) -> {
            Gauge.builder("cis.kafka.connections", () -> connections(listenerId))
                    .description("Open broker connections of the crane event consumers")
                    .tag("source", scope.id())
                    .register(meters);
            Gauge.builder("cis.kafka.partitions", () -> partitions(listenerId))
                    .description("Partitions assigned to the crane event consumers")
                    .tag("source", scope.id())
                    .register(meters);
        });
    }

    private double connections(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container == null) {
            return 0;
        }
        double total = 0;
        for (Map<MetricName, ? extends Metric> metrics : container.metrics().values()) {
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                MetricName name = entry.getKey();
                if (name.name().equals("connection-count") && name.group().equals("consumer-metrics")
                        && entry.getValue().metricValue() instanceof Number value) {
                    total += value.doubleValue();
                }
            }
        }
        return total;
    }

    private double partitions(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        Collection<TopicPartition> assigned = container == null ? null : container.getAssignedPartitions();
        return assigned == null ? 0 : assigned.size();
    }
}
