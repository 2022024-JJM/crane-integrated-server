package com.hanwha.cis.config;

import com.hanwha.cis.adapter.in.websocket.RedisSnapshotSubscriber;
import io.lettuce.core.ClientOptions;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientOptionsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Redis 채널 구독 컨테이너. 리스너 호출은 single-thread executor "redis-sub-" 에서 순서대로 실행되고,
 * 거기서 SimpMessagingTemplate 로 STOMP 푸시를 한다. 테스트 프로필에서는 Redis 자체를 쓰지 않는다.
 * Redis는 없어도 되는 의존성이다: 컨테이너는 자동 시작하지 않고 {@link RedisSubscriptionStarter}가 띄우며(Redis 없이도 서버가 뜨게),
 * 끊긴 동안의 명령은 기다리지 않고 바로 실패시킨다. 타임아웃은 spring.data.redis.connect-timeout / timeout.
 */
@Configuration
@Profile("!test")
public class RedisConfig {

    @Bean
    RedisMessageListenerContainer cisRedisListenerContainer(RedisConnectionFactory connectionFactory,
            RedisSnapshotSubscriber subscriber, CisProperties properties) {
        ThreadPoolTaskExecutor dispatcher = new ThreadPoolTaskExecutor();
        dispatcher.setThreadNamePrefix("redis-sub-");
        dispatcher.setCorePoolSize(1);
        dispatcher.setMaxPoolSize(1);
        dispatcher.setDaemon(true);
        dispatcher.initialize();

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setTaskExecutor(dispatcher);
        container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor("redis-sub-conn-"));
        container.addMessageListener(subscriber, new ChannelTopic(properties.redis().snapshotChannel()));
        container.setAutoStartup(false);
        return container;
    }

    /**
     * 연결이 끊긴 동안 명령을 큐에 쌓지 않고 바로 실패시킨다. Lettuce 기본값은 쌓아 두고 timeout까지 기다리므로
     * 푸시 스레드와 REST 조회가 그동안 멈춘다. 이 서버는 실패하면 다음 틱 재시도·메모리 상태 응답으로 넘어간다.
     */
    @Bean
    LettuceClientOptionsBuilderCustomizer cisRejectCommandsWhileDisconnected() {
        return builder -> builder.disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS);
    }

    @Bean
    RedisSubscriptionStarter cisRedisSubscriptionStarter(RedisMessageListenerContainer cisRedisListenerContainer,
            CisProperties properties) {
        return new RedisSubscriptionStarter(cisRedisListenerContainer, properties.redis().snapshotChannel());
    }
}
