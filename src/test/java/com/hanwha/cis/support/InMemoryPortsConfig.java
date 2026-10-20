package com.hanwha.cis.support;

import com.hanwha.cis.adapter.in.websocket.SnapshotStompBroadcaster;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * test 프로필에서 Redis 어댑터(@Profile("!test"))를 대신한다. publisher는 STOMP 브로드캐스터를 직접 호출해
 * Redis 없이도 푸시 경로가 끝까지 이어지게 한다.
 */
@TestConfiguration
public class InMemoryPortsConfig {

    @Bean
    InMemoryCraneStateStore craneStateStore() {
        return new InMemoryCraneStateStore();
    }

    @Bean
    RecordingSnapshotPublisher snapshotPublisher(SnapshotStompBroadcaster broadcaster) {
        return new RecordingSnapshotPublisher(broadcaster::broadcast);
    }
}
