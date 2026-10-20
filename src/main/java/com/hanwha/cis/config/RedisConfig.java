package com.hanwha.cis.config;

import com.hanwha.cis.adapter.in.websocket.RedisSnapshotSubscriber;
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
        return container;
    }
}
