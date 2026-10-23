package com.hanwha.cis.adapter.in.websocket;

import com.hanwha.cis.application.support.FailureLog;
import com.hanwha.cis.domain.model.CraneSnapshot;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Redis 채널(cis.redis.snapshot-channel)의 스냅샷 묶음 JSON을 받아 STOMP로 넘긴다. 전용 executor "redis-sub-" 에서 실행된다.
 * 넘긴 메시지 수는 cis.push.forwarded 로 노출한다(푸시 감시와 요약 로그가 발행 수 cis.push.published 와 비교한다).
 */
@Component
@Profile("!test")
public class RedisSnapshotSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisSnapshotSubscriber.class);
    private static final TypeReference<List<CraneSnapshot>> SNAPSHOT_LIST = new TypeReference<>() { };

    private final SnapshotStompBroadcaster broadcaster;
    private final JsonMapper json;
    private final FailureLog failureLog = new FailureLog(log, "Snapshot forward from Redis to STOMP");
    private final LongAdder forwarded = new LongAdder();

    public RedisSnapshotSubscriber(SnapshotStompBroadcaster broadcaster, JsonMapper json, MeterRegistry meters) {
        this.broadcaster = broadcaster;
        this.json = json;
        FunctionCounter.builder("cis.push.forwarded", forwarded, LongAdder::sum)
                .description("Snapshot messages forwarded from Redis to STOMP")
                .register(meters);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            List<CraneSnapshot> snapshots = json.readValue(message.getBody(), SNAPSHOT_LIST);
            broadcaster.broadcast(snapshots);
            forwarded.increment();
            failureLog.success();
        } catch (RuntimeException e) {
            failureLog.failure(e);
        }
    }
}
