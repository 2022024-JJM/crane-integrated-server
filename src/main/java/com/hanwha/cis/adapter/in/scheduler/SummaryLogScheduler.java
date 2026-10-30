package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.config.SchedulingConfig;
import com.hanwha.cis.domain.model.CraneScope;
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
 * 60초마다 요약 한 줄. 카운터는 Micrometer에서 읽어 직전 로그 이후 증가분을 찍는다. 수신은 indoor·outdoor를 나눠 찍는다.
 * 예: ingest indoor received=53100 changed=1200 skipped=0 (parse=0 mismatch=0 late=0) null=0 partitions=3
 *     | outdoor received=117900 changed=5200 skipped=0 (parse=0 mismatch=0 late=0) null=1680 partitions=1
 *       common=15300 unassigned=37620 | kafka connections=4
 *     | history rows=2340/1500 queue=0 failures=0 dropped=0 | push published=240 forwarded=240 | ws sessions=1
 *     | cranes online=39/39
 * outdoor의 common은 크레인 슬롯 밖 공통 블록, unassigned는 Crane_ID가 0인 슬롯의 태그 수다(둘 다 received에 포함).
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
        String outdoor = CraneScope.OUTDOOR.id();
        log.info("ingest indoor {} | outdoor {} common={} unassigned={} | kafka connections={}"
                        + " | history rows={}/{} queue={} failures={} dropped={}"
                        + " | push published={} forwarded={} | ws sessions={} | cranes online={}/{}",
                ingest(CraneScope.INDOOR.id()), ingest(outdoor),
                delta("cis.ingest.ignored", "source", outdoor, "reason", "common"),
                delta("cis.ingest.ignored", "source", outdoor, "reason", "unassigned"),
                gaugeSum("cis.kafka.connections"),
                delta("cis.history.rows", "table", "crane_history"),
                delta("cis.history.rows", "table", "crane_history_diff"),
                gauge("cis.history.queue_size"), delta("cis.history.write_failures"), delta("cis.history.dropped"),
                delta("cis.push.published"), delta("cis.push.forwarded"),
                gauge("cis.ws.sessions"), gauge("cis.cranes.online"), gauge("cis.cranes.total"));
    }

    /** 토픽 하나의 수신 카운터 증가분과 할당 파티션 수. */
    private String ingest(String source) {
        return "received=%d changed=%d skipped=%d (parse=%d mismatch=%d late=%d) null=%d partitions=%d".formatted(
                delta("cis.ingest.received", "source", source), delta("cis.ingest.changed", "source", source),
                delta("cis.ingest.skipped", "source", source),
                delta("cis.ingest.skipped", "source", source, "reason", "parse"),
                delta("cis.ingest.skipped", "source", source, "reason", "mismatch"),
                delta("cis.ingest.skipped", "source", source, "reason", "late"),
                delta("cis.ingest.nulls", "source", source), gauge("cis.kafka.partitions", "source", source));
    }

    private long delta(String name, String... tags) {
        double total = meters.find(name).tags(tags).functionCounters().stream()
                .mapToDouble(FunctionCounter::count).sum();
        String key = name + String.join(",", tags);
        double before = previous.getOrDefault(key, 0.0);
        previous.put(key, total);
        return Math.round(total - before);
    }

    private long gauge(String name, String... tags) {
        Gauge gauge = meters.find(name).tags(tags).gauge();
        return gauge == null ? 0 : Math.round(gauge.value());
    }

    private long gaugeSum(String name) {
        return Math.round(meters.find(name).gauges().stream().mapToDouble(Gauge::value).sum());
    }
}
