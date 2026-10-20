package com.hanwha.cis.adapter.in.websocket;

import com.hanwha.cis.config.CisProperties;
import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import tools.jackson.databind.json.JsonMapper;

/**
 * STOMP over WebSocket. 엔드포인트 /ws(SockJS 없음), 심플 브로커 /topic.
 * 구독 직후 초기 상태는 REST GET /api/cranes 로 받는다.
 */
@Configuration
@EnableWebSocketMessageBroker
public class StompConfig implements WebSocketMessageBrokerConfigurer {

    private final CisProperties properties;
    private final JsonMapper json;

    public StompConfig(CisProperties properties, JsonMapper json) {
        this.properties = properties;
        this.json = json;
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns(properties.web().allowedOrigins().toArray(String[]::new));
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public boolean configureMessageConverters(List<MessageConverter> converters) {
        converters.add(new JacksonJsonMessageConverter(json));
        return false;
    }
}
