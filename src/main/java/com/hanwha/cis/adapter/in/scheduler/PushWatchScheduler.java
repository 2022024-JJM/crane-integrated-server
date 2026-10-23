package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.application.support.Durations;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.config.SchedulingConfig;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * 5초마다 실시간 푸시 경로(Redis 발행 → 구독 → STOMP)가 이어지는지 본다. 발행한 메시지가 30초 동안 하나도 구독으로 돌아오지 않으면
 * WARN(이어지면 1분마다 한 줄), 다시 돌아오면 INFO. 구독이 다시 붙지 않거나 전달 스레드가 막히는 것처럼 예외 없이 멈추는 경우를 잡기 위함이다.
 * 발행 자체가 실패하는 Redis 장애는 푸시 스케줄러의 실패 로그가 남기므로 여기서는 보지 않는다(발행 수가 늘지 않는다).
 * 값은 Micrometer(cis.push.published, cis.push.forwarded)에서 읽는다. 테스트 프로필에는 Redis 구독이 없어 띄우지 않는다.
 */
@Component
@Profile("!test")
public class PushWatchScheduler extends FixedDelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(PushWatchScheduler.class);
    static final Duration INTERVAL = Duration.ofSeconds(5);
    static final Duration STALL_THRESHOLD = Duration.ofSeconds(30);
    static final Duration REMIND_EVERY = Duration.ofSeconds(60);

    private final MeterRegistry meters;
    private final Clock clock;
    private final String channel;

    private double lastForwarded;
    /** 전달 수가 마지막으로 늘었을 때의 발행 수. 발행이 이보다 늘었으면 아직 돌아오지 않은 메시지가 있다. */
    private double publishedAtLastForward;
    /** 돌아오지 않은 발행을 처음 본 시각. */
    private Instant pendingSince;
    private boolean stalled;
    private Instant lastLoggedAt;

    @Autowired
    public PushWatchScheduler(MeterRegistry meters, Clock clock, CisProperties properties,
            @Qualifier(SchedulingConfig.PUSH_WATCH_SCHEDULER) ThreadPoolTaskScheduler scheduler) {
        this(meters, clock, properties.redis().snapshotChannel(), scheduler);
    }

    PushWatchScheduler(MeterRegistry meters, Clock clock, String channel, ThreadPoolTaskScheduler scheduler) {
        super(scheduler, INTERVAL, "Live push watch");
        this.meters = meters;
        this.clock = clock;
        this.channel = channel;
    }

    boolean stalled() {
        return stalled;
    }

    @Override
    protected void tick() {
        Instant now = clock.instant();
        double published = counter("cis.push.published");
        double forwarded = counter("cis.push.forwarded");
        if (forwarded > lastForwarded) {
            if (stalled) {
                stalled = false;
                log.info("Live push resumed after {}", Durations.format(Duration.between(pendingSince, now)));
            }
            lastForwarded = forwarded;
            publishedAtLastForward = published;
            pendingSince = null;
            return;
        }
        if (published <= publishedAtLastForward) {
            return;
        }
        if (pendingSince == null) {
            pendingSince = now;
        }
        Duration pending = Duration.between(pendingSince, now);
        if (pending.compareTo(STALL_THRESHOLD) < 0) {
            return;
        }
        long undelivered = Math.round(published - publishedAtLastForward);
        if (!stalled) {
            stalled = true;
            lastLoggedAt = now;
            log.warn("Live push stalled: {} message(s) published to Redis channel '{}' for {} but none came back"
                    + " through the subscription, so STOMP clients get no updates. Check the Redis subscription"
                    + " and the redis-sub- threads.", undelivered, channel, Durations.format(pending));
        } else if (!now.isBefore(lastLoggedAt.plus(REMIND_EVERY))) {
            lastLoggedAt = now;
            log.warn("Live push still stalled for {} ({} message(s) not delivered)", Durations.format(pending),
                    undelivered);
        }
    }

    private double counter(String name) {
        return meters.find(name).functionCounters().stream().mapToDouble(FunctionCounter::count).sum();
    }
}
