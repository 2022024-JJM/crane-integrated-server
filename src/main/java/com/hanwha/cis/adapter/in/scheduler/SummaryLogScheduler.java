package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.config.SchedulingConfig;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * 60초마다 요약 한 줄. 카운터는 Micrometer에서 읽어 직전 로그 이후 증가분을 찍는다.
 * 예: ingest received=53100 changed=1200 skipped=0 (parse=0 mismatch=0 late=0) | kafka connections=3 partitions=3
 *     | history rows=180/12 queue=0 failures=0 dropped=0 | push published=240 forwarded=240 | ws sessions=1
 *     | cranes online=3/3
 * push는 Redis 채널에 발행한 메시지 수와 구독으로 받아 STOMP로 넘긴 메시지 수다(정상이면 같다).
 */
@Component
public class SummaryLogScheduler extends FixedDelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(SummaryLogScheduler.class);
    static final Duration INTERVAL = Duration.ofSeconds(60);

    private final MeterRegistry meters;
    private final Map<String, Double> previous = new HashMap<>();

    public SummaryLogScheduler(MeterRegistry meters,
            @Qualifier(SchedulingConfig.SUMMARY_SCHEDULER) ThreadPoolTaskScheduler scheduler) {
        super(scheduler, INTERVAL, "Summary log");
        this.meters = meters;
    }

    @Override
    protected void tick() {
        log.info("ingest received={} changed={} skipped={} (parse={} mismatch={} late={})"
                        + " | kafka connections={} partitions={}"
                        + " | history rows={}/{} queue={} failures={} dropped={}"
                        + " | push published={} forwarded={} | ws sessions={} | cranes online={}/{}",
                delta("cis.ingest.received"), delta("cis.ingest.changed"), delta("cis.ingest.skipped"),
                delta("cis.ingest.skipped", "reason", "parse"), delta("cis.ingest.skipped", "reason", "mismatch"),
                delta("cis.ingest.skipped", "reason", "late"),
                gauge("cis.kafka.connections"), gauge("cis.kafka.partitions"),
                delta("cis.history.rows", "table", "crane_history"),
                delta("cis.history.rows", "table", "crane_history_diff"),
                gauge("cis.history.queue_size"), delta("cis.history.write_failures"), delta("cis.history.dropped"),
                delta("cis.push.published"), delta("cis.push.forwarded"),
                gauge("cis.ws.sessions"), gauge("cis.cranes.online"), gauge("cis.cranes.total"));
    }

    private long delta(String name, String... tags) {
        double total = meters.find(name).tags(tags).functionCounters().stream()
                .mapToDouble(FunctionCounter::count).sum();
        String key = name + String.join(",", tags);
        double before = previous.getOrDefault(key, 0.0);
        previous.put(key, total);
        return Math.round(total - before);
    }

    private long gauge(String name) {
        Gauge gauge = meters.find(name).gauge();
        return gauge == null ? 0 : Math.round(gauge.value());
    }
}
