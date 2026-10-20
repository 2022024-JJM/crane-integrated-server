package com.hanwha.cis.adapter.in.websocket;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

/** 연결된 STOMP 세션 수를 게이지 cis.ws.sessions 로 노출한다. DisconnectEvent는 중복 발생할 수 있어 세션 ID 집합으로 센다. */
@Component
public class WebSocketSessionMetrics {

    private final Set<String> sessions = ConcurrentHashMap.newKeySet();

    public WebSocketSessionMetrics(MeterRegistry meters) {
        Gauge.builder("cis.ws.sessions", sessions, Set::size)
                .description("Connected STOMP sessions")
                .register(meters);
    }

    @EventListener
    public void onConnected(SessionConnectedEvent event) {
        String id = StompHeaderAccessor.wrap(event.getMessage()).getSessionId();
        if (id != null) {
            sessions.add(id);
        }
    }

    @EventListener
    public void onDisconnected(SessionDisconnectEvent event) {
        sessions.remove(event.getSessionId());
    }

    public int sessions() {
        return sessions.size();
    }
}
