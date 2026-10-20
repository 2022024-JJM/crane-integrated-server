package com.hanwha.cis.adapter.out.redis;

import com.hanwha.cis.application.port.out.SnapshotPublisherPort;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** 채널 cis:crane:snapshots 에 틱당 1회, 변한 스냅샷 목록 JSON을 publish 한다. */
@Component
@Profile("!test")
public class RedisSnapshotPublisher implements SnapshotPublisherPort {

    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final String channel;

    public RedisSnapshotPublisher(StringRedisTemplate redis, JsonMapper json, CisProperties properties) {
        this.redis = redis;
        this.json = json;
        this.channel = properties.redis().snapshotChannel();
    }

    @Override
    public void publish(List<CraneSnapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return;
        }
        redis.convertAndSend(channel, json.writeValueAsString(snapshots));
    }
}
