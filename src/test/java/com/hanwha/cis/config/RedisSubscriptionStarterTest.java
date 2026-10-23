package com.hanwha.cis.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.hanwha.cis.support.LogCapture;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

class RedisSubscriptionStarterTest {

    @Test
    void retriesInBackgroundUntilSubscribed() {
        FlakyContainer container = new FlakyContainer(3);
        RedisSubscriptionStarter starter = new RedisSubscriptionStarter(container, "cis:crane:snapshots",
                Duration.ofMillis(10));

        try (LogCapture logs = LogCapture.of(RedisSubscriptionStarter.class)) {
            starter.start();

            await().atMost(Duration.ofSeconds(5)).until(() -> container.listening);
            assertThat(container.attempts.get()).isEqualTo(4);
            assertThat(container.stops.get()).isEqualTo(3);
            await().atMost(Duration.ofSeconds(5)).until(() -> logs.lines().size() == 3);
            assertThat(logs.lines().get(0)).startsWith("WARN Redis subscription to cis:crane:snapshots failed:"
                    + " RedisConnectionFailureException: Unable to connect to Redis");
            assertThat(logs.lines().get(1)).startsWith("INFO Redis subscription to cis:crane:snapshots recovered");
            assertThat(logs.lines().get(2)).isEqualTo("INFO Subscribed to Redis channel cis:crane:snapshots");
        } finally {
            starter.stop();
        }
        assertThat(starter.isRunning()).isFalse();
    }

    /** 처음 failures번은 Redis 연결 실패처럼 start()에서 던지는 컨테이너. */
    private static final class FlakyContainer extends RedisMessageListenerContainer {
        private final int failures;
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger stops = new AtomicInteger();
        private volatile boolean listening;

        FlakyContainer(int failures) {
            this.failures = failures;
        }

        @Override
        public void start() {
            if (attempts.incrementAndGet() <= failures) {
                throw new RedisConnectionFailureException("Unable to connect to Redis");
            }
            listening = true;
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
        }
    }
}
