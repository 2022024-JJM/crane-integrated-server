package com.hanwha.cis.config;

import com.hanwha.cis.adapter.out.persistence.HistoryWriter;
import com.hanwha.cis.application.service.CraneStateRegistry;
import com.hanwha.cis.application.service.IngestStats;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.time.Clock;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 애플리케이션·어댑터 카운터를 Micrometer에 바인딩한다(애플리케이션 계층은 Micrometer를 모른다).
 * cis.ingest.received / changed / skipped{reason}, cis.history.rows{table} / write_failures / dropped / queue_size,
 * cis.cranes.online / total. (cis.ws.sessions 는 websocket 어댑터가 직접 등록)
 */
@Configuration
public class MetricsConfig {

    @Bean
    MeterBinder cisMeters(IngestStats ingest, HistoryWriter writer, CraneStateRegistry registry, Clock clock,
            CisProperties properties) {
        Duration stale = properties.state().staleThreshold();
        return meters -> {
            FunctionCounter.builder("cis.ingest.received", ingest, IngestStats::received).register(meters);
            FunctionCounter.builder("cis.ingest.changed", ingest, IngestStats::changed).register(meters);
            for (SkipReason reason : SkipReason.values()) {
                FunctionCounter.builder("cis.ingest.skipped", ingest, s -> s.skipped(reason))
                        .tag("reason", reason.tag()).register(meters);
            }
            FunctionCounter.builder("cis.history.rows", writer, HistoryWriter::historyRows)
                    .tag("table", "crane_history").register(meters);
            FunctionCounter.builder("cis.history.rows", writer, HistoryWriter::diffRows)
                    .tag("table", "crane_history_diff").register(meters);
            FunctionCounter.builder("cis.history.write_failures", writer, HistoryWriter::writeFailures)
                    .register(meters);
            FunctionCounter.builder("cis.history.dropped", writer, HistoryWriter::dropped).register(meters);
            Gauge.builder("cis.history.queue_size", writer, HistoryWriter::queueSize).register(meters);
            Gauge.builder("cis.cranes.online", registry, r -> r.onlineCount(clock.instant(), stale))
                    .register(meters);
            Gauge.builder("cis.cranes.total", registry, CraneStateRegistry::size).register(meters);
        };
    }
}
