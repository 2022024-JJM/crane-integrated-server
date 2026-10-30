package com.hanwha.cis.config;

import com.hanwha.cis.application.port.out.CraneHistoryPort;
import com.hanwha.cis.application.port.out.CraneRegistryPort;
import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.application.port.out.SnapshotPublisherPort;
import com.hanwha.cis.application.service.CraneHistoryQueryService;
import com.hanwha.cis.application.service.CraneStateQueryService;
import com.hanwha.cis.application.service.CraneStateRegistry;
import com.hanwha.cis.application.service.HistoryRecordingService;
import com.hanwha.cis.application.service.IngestStats;
import com.hanwha.cis.application.service.OutdoorTagIngestionService;
import com.hanwha.cis.application.service.SnapshotPushService;
import com.hanwha.cis.application.service.TagIngestionService;
import com.hanwha.cis.domain.model.CraneScope;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 애플리케이션 계층(프레임워크 비의존)의 빈 조립. 설정값은 여기서만 풀어 넘긴다. */
@Configuration
@EnableConfigurationProperties(CisProperties.class)
public class ApplicationConfig {

    /** 수신 카운터는 scope(indoor/outdoor)마다 하나다. 빈 이름으로 골라 넣는다. */
    public static final String INDOOR_INGEST_STATS = "indoorIngestStats";
    public static final String OUTDOOR_INGEST_STATS = "outdoorIngestStats";

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean(initMethod = "restore")
    CraneStateRegistry craneStateRegistry(CraneStateStorePort store) {
        return new CraneStateRegistry(store);
    }

    @Bean(INDOOR_INGEST_STATS)
    IngestStats indoorIngestStats() {
        return new IngestStats(CraneScope.INDOOR);
    }

    @Bean(OUTDOOR_INGEST_STATS)
    IngestStats outdoorIngestStats() {
        return new IngestStats(CraneScope.OUTDOOR);
    }

    @Bean
    TagIngestionService tagIngestionService(CraneStateRegistry registry,
            @Qualifier(INDOOR_INGEST_STATS) IngestStats stats, Clock clock) {
        return new TagIngestionService(registry, stats, clock);
    }

    @Bean
    OutdoorTagIngestionService outdoorTagIngestionService(CraneStateRegistry registry,
            @Qualifier(OUTDOOR_INGEST_STATS) IngestStats stats, Clock clock) {
        return new OutdoorTagIngestionService(registry, stats, clock);
    }

    @Bean
    SnapshotPushService snapshotPushService(CraneStateRegistry registry, CraneStateStorePort store,
            SnapshotPublisherPort publisher, Clock clock, CisProperties properties) {
        return new SnapshotPushService(registry, store, publisher, clock, properties.state().staleThreshold());
    }

    @Bean
    HistoryRecordingService historyRecordingService(CraneStateRegistry registry, CraneHistoryPort history,
            CraneRegistryPort cranes, Clock clock, CisProperties properties) {
        return new HistoryRecordingService(registry, history, cranes, clock, properties.state().staleThreshold());
    }

    @Bean
    CraneStateQueryService craneStateQueryService(CraneStateRegistry registry, CraneStateStorePort store,
            Clock clock, CisProperties properties) {
        return new CraneStateQueryService(registry, store, clock, properties.state().staleThreshold());
    }

    @Bean
    CraneHistoryQueryService craneHistoryQueryService(CraneHistoryPort history, CraneRegistryPort cranes,
            CraneStateRegistry registry, Clock clock, CisProperties properties) {
        CisProperties.Query query = properties.query();
        return new CraneHistoryQueryService(history, cranes, registry, clock, new CraneHistoryQueryService.Limits(
                query.maxRange(), query.defaultStep(), query.maxPoints(), query.defaultChangeLimit(),
                query.maxChangeLimit()));
    }
}
