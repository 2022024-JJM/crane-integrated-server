package com.hanwha.cis.adapter.in.websocket;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** Redis 채널(cis.redis.snapshot-channel)의 스냅샷 묶음 JSON을 받아 STOMP로 넘긴다. 전용 executor "redis-sub-" 에서 실행된다. */
@Component
@Profile("!test")
public class RedisSnapshotSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisSnapshotSubscriber.class);
    private static final TypeReference<List<CraneSnapshot>> SNAPSHOT_LIST = new TypeReference<>() { };

    private final SnapshotStompBroadcaster broadcaster;
    private final JsonMapper json;

    public RedisSnapshotSubscriber(SnapshotStompBroadcaster broadcaster, JsonMapper json) {
        this.broadcaster = broadcaster;
        this.json = json;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            List<CraneSnapshot> snapshots = json.readValue(message.getBody(), SNAPSHOT_LIST);
            broadcaster.broadcast(snapshots);
        } catch (RuntimeException e) {
            log.warn("Failed to forward snapshot message from Redis: {}", e.toString());
        }
    }
}
