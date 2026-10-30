package com.hanwha.cis.adapter.in.websocket;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.hanwha.cis.adapter.in.websocket.SnapshotStompBroadcaster.SnapshotMessage;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.simp.SimpMessagingTemplate;

class SnapshotStompBroadcasterTest {

    private static final Instant T0 = Instant.parse("2026-10-30T04:47:50.019Z");

    private final SimpMessagingTemplate messaging = mock(SimpMessagingTemplate.class);
    private final SnapshotStompBroadcaster broadcaster =
            new SnapshotStompBroadcaster(messaging, Clock.fixed(T0, ZoneOffset.UTC));

    private static CraneSnapshot snapshot(String craneId, CraneScope scope) {
        return new CraneSnapshot(craneId, scope, "isl-ocean-crane", "bay", T0, T0, true, Map.of());
    }

    @Test
    void sendsEachScopeToItsOwnTopics() {
        CraneSnapshot indoor = snapshot("C_867", CraneScope.INDOOR);
        CraneSnapshot outdoor = snapshot("GC_01", CraneScope.OUTDOOR);

        broadcaster.broadcast(List.of(indoor, outdoor));

        verify(messaging).convertAndSend("/topic/cranes", new SnapshotMessage("snapshot", T0, List.of(indoor)));
        verify(messaging).convertAndSend("/topic/cranes/C_867", indoor);
        verify(messaging).convertAndSend("/topic/outdoor/cranes",
                new SnapshotMessage("snapshot", T0, List.of(outdoor)));
        verify(messaging).convertAndSend("/topic/outdoor/cranes/GC_01", outdoor);
        verifyNoMoreInteractions(messaging);
    }

    @Test
    void sendsNothingForAnEmptyBatch() {
        broadcaster.broadcast(List.of());

        verifyNoMoreInteractions(messaging);
    }
}
