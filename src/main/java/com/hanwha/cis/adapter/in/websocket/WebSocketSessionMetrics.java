package com.hanwha.cis.adapter.in.websocket;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.server.HandshakeInterceptor;

/**
 * 연결된 STOMP 세션 수를 게이지 cis.ws.sessions 로 노출하고 접속·해제를 INFO로 남긴다.
 * DisconnectEvent는 중복 발생할 수 있어 세션 ID 집합으로 센다. 원격 주소는 핸드셰이크 때 세션 속성에 넣어 둔다.
 */
@Component
public class WebSocketSessionMetrics {

    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionMetrics.class);
    static final String REMOTE_ADDRESS = "cis.remoteAddress";

    private final Set<String> sessions = ConcurrentHashMap.newKeySet();

    public WebSocketSessionMetrics(MeterRegistry meters) {
        Gauge.builder("cis.ws.sessions", sessions, Set::size)
                .description("Connected STOMP sessions")
                .register(meters);
    }

    @EventListener
    public void onConnected(SessionConnectedEvent event) {
        String id = StompHeaderAccessor.wrap(event.getMessage()).getSessionId();
        if (id != null && sessions.add(id)) {
            // CONNECTED 프레임에는 세션 속성이 없고, 원래 CONNECT 메시지가 헤더로 따라온다.
            Object connect = event.getMessage().getHeaders().get(SimpMessageHeaderAccessor.CONNECT_MESSAGE_HEADER);
            String remote = connect instanceof Message<?> message ? remoteAddress(message.getHeaders()) : "?";
            log.info("STOMP client connected: session={} remote={} (sessions={})", id, remote, sessions.size());
        }
    }

    @EventListener
    public void onDisconnected(SessionDisconnectEvent event) {
        if (sessions.remove(event.getSessionId())) {
            log.info("STOMP client disconnected: session={} remote={} closeCode={} (sessions={})",
                    event.getSessionId(), remoteAddress(event.getMessage().getHeaders()),
                    event.getCloseStatus().getCode(), sessions.size());
        }
    }

    public int sessions() {
        return sessions.size();
    }

    private static String remoteAddress(Map<String, Object> headers) {
        Map<String, Object> attributes = SimpMessageHeaderAccessor.getSessionAttributes(headers);
        Object address = attributes == null ? null : attributes.get(REMOTE_ADDRESS);
        return address == null ? "?" : address.toString();
    }

    /** 핸드셰이크 요청의 원격 주소(ip:port)를 세션 속성에 넣는다. */
    static final class RemoteAddressInterceptor implements HandshakeInterceptor {

        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                WebSocketHandler handler, Map<String, Object> attributes) {
            InetSocketAddress address = request.getRemoteAddress();
            if (address != null) {
                attributes.put(REMOTE_ADDRESS, address.getHostString() + ":" + address.getPort());
            }
            return true;
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                WebSocketHandler handler, Exception exception) {
        }
    }
}
