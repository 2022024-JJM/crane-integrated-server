package com.hanwha.cis.adapter.in.kafka;

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
 * 수신 감시(IngestWatchScheduler)와 요약 로그가 읽는다. 컨슈머 metrics()는 잠금 없이 읽혀 다른 스레드에서 불러도 된다.
 */
@Component
public class KafkaListenerMetrics implements MeterBinder {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaListenerMetrics(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void bindTo(MeterRegistry meters) {
        Gauge.builder("cis.kafka.connections", this, KafkaListenerMetrics::connections)
                .description("Open broker connections of the crane event consumers")
                .register(meters);
        Gauge.builder("cis.kafka.partitions", this, KafkaListenerMetrics::partitions)
                .description("Partitions assigned to the crane event consumers")
                .register(meters);
    }

    private double connections() {
        MessageListenerContainer container = registry.getListenerContainer(CraneEventKafkaListener.LISTENER_ID);
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

    private double partitions() {
        MessageListenerContainer container = registry.getListenerContainer(CraneEventKafkaListener.LISTENER_ID);
        Collection<TopicPartition> assigned = container == null ? null : container.getAssignedPartitions();
        return assigned == null ? 0 : assigned.size();
    }
}
