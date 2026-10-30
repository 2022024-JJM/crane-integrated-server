package com.hanwha.cis.adapter.in.websocket;

import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * 스냅샷 묶음을 scope별 STOMP 토픽으로 보낸다. 묶음 토픽에 1건, 그 아래 /{craneId} 에 크레인별 1건.
 * indoor는 /topic/cranes, outdoor는 /topic/outdoor/cranes. 한 묶음에 두 scope가 섞여 오면 나눠 보낸다.
 * 운영에서는 Redis 구독 스레드가, 테스트에서는 publisher 대체 구현이 호출한다.
 */
@Component
public class SnapshotStompBroadcaster {

    public static final String TOPIC_ALL = "/topic/cranes";
    public static final String OUTDOOR_TOPIC_ALL = "/topic/outdoor/cranes";

    /** 묶음 토픽 메시지 형식. */
    public record SnapshotMessage(String type, Instant sentAt, List<CraneSnapshot> cranes) {
    }

    private final SimpMessagingTemplate messaging;
    private final Clock clock;

    public SnapshotStompBroadcaster(SimpMessagingTemplate messaging, Clock clock) {
        this.messaging = messaging;
        this.clock = clock;
    }

    public void broadcast(List<CraneSnapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return;
        }
        Map<CraneScope, List<CraneSnapshot>> byScope = new EnumMap<>(CraneScope.class);
        for (CraneSnapshot snapshot : snapshots) {
            byScope.computeIfAbsent(snapshot.scope(), scope -> new ArrayList<>()).add(snapshot);
        }
        Instant sentAt = clock.instant();
        byScope.forEach((scope, cranes) -> {
            String topic = topic(scope);
            messaging.convertAndSend(topic, new SnapshotMessage("snapshot", sentAt, cranes));
            for (CraneSnapshot snapshot : cranes) {
                messaging.convertAndSend(topic + "/" + snapshot.craneId(), snapshot);
            }
        });
    }

    static String topic(CraneScope scope) {
        return switch (scope) {
            case INDOOR -> TOPIC_ALL;
            case OUTDOOR -> OUTDOOR_TOPIC_ALL;
        };
    }
}
