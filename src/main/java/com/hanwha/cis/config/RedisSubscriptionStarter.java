package com.hanwha.cis.config;

import com.hanwha.cis.application.support.FailureLog;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Redis 채널 구독을 백그라운드 스레드에서 성공할 때까지 재시도한다.
 * Spring Data Redis는 컨테이너의 첫 구독 실패를 기동 실패로 던지므로, 그대로 두면 Redis 없이는 서버가 뜨지 않는다.
 * 수집·이력·REST는 Redis 없이도 동작하므로 컨테이너 자동 시작을 끄고 여기서 시작한다. 한 번 구독된 뒤의 끊김은 컨테이너가 재구독한다.
 */
final class RedisSubscriptionStarter implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RedisSubscriptionStarter.class);
    static final Duration RETRY_INTERVAL = Duration.ofSeconds(5);

    private final RedisMessageListenerContainer container;
    private final String channel;
    private final Duration retryInterval;
    private final FailureLog failureLog;
    private volatile Thread thread;

    RedisSubscriptionStarter(RedisMessageListenerContainer container, String channel) {
        this(container, channel, RETRY_INTERVAL);
    }

    RedisSubscriptionStarter(RedisMessageListenerContainer container, String channel, Duration retryInterval) {
        this.container = container;
        this.channel = channel;
        this.retryInterval = retryInterval;
        this.failureLog = new FailureLog(log, "Redis subscription to " + channel);
    }

    @Override
    public void start() {
        if (thread != null) {
            return;
        }
        Thread t = new Thread(this::subscribeUntilListening, "redis-sub-start");
        t.setDaemon(true);
        thread = t;
        t.start();
    }

    private void subscribeUntilListening() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                container.start();
                failureLog.success();
                log.info("Subscribed to Redis channel {}", channel);
                return;
            } catch (RuntimeException e) {
                container.stop();
                failureLog.failure(e);
            }
            try {
                Thread.sleep(retryInterval.toMillis());
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override
    public void stop() {
        Thread t = thread;
        thread = null;
        if (t != null) {
            t.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return thread != null;
    }
}
