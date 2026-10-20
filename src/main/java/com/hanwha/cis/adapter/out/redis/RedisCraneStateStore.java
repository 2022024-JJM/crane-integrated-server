package com.hanwha.cis.adapter.out.redis;

import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** HASH cis:crane:state — field=craneId, value=CraneSnapshot JSON. TTL 없음(stale은 online 플래그로 판단). */
@Component
@Profile("!test")
public class RedisCraneStateStore implements CraneStateStorePort {

    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final String key;

    public RedisCraneStateStore(StringRedisTemplate redis, JsonMapper json, CisProperties properties) {
        this.redis = redis;
        this.json = json;
        this.key = properties.redis().stateKey();
    }

    @Override
    public void save(List<CraneSnapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return;
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (CraneSnapshot snapshot : snapshots) {
            fields.put(snapshot.craneId(), json.writeValueAsString(snapshot));
        }
        redis.opsForHash().putAll(key, fields);
    }

    @Override
    public List<CraneSnapshot> findAll() {
        return redis.<String, String>opsForHash().entries(key).values().stream()
                .map(value -> json.readValue(value, CraneSnapshot.class))
                .toList();
    }

    @Override
    public Optional<CraneSnapshot> find(String craneId) {
        String value = redis.<String, String>opsForHash().get(key, craneId);
        return Optional.ofNullable(value).map(v -> json.readValue(v, CraneSnapshot.class));
    }
}
