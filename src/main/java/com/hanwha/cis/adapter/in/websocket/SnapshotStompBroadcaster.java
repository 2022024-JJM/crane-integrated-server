package com.hanwha.cis.adapter.in.websocket;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * 스냅샷 묶음을 STOMP 토픽으로 보낸다: /topic/cranes 에 묶음 1건, /topic/cranes/{craneId} 에 크레인별 1건.
 * 운영에서는 Redis 구독 스레드가, 테스트에서는 publisher 대체 구현이 호출한다.
 */
@Component
public class SnapshotStompBroadcaster {

    public static final String TOPIC_ALL = "/topic/cranes";
    public static final String TOPIC_CRANE_PREFIX = "/topic/cranes/";

    /** /topic/cranes 메시지 형식. */
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
        messaging.convertAndSend(TOPIC_ALL, new SnapshotMessage("snapshot", clock.instant(), snapshots));
        for (CraneSnapshot snapshot : snapshots) {
            messaging.convertAndSend(TOPIC_CRANE_PREFIX + snapshot.craneId(), snapshot);
        }
    }
}
