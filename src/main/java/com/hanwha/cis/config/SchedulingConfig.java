package com.hanwha.cis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 주기 작업마다 전용 single-thread 스케줄러를 둔다.
 * 푸시·이력·요약 작업이 서로의 지연에 영향을 받지 않게 하기 위함이다.
 */
@Configuration
public class SchedulingConfig {

    public static final String PUSH_SCHEDULER = "pushScheduler";
    public static final String HISTORY_SCHEDULER = "historyScheduler";
    public static final String SUMMARY_SCHEDULER = "summaryScheduler";
    public static final String WATCH_SCHEDULER = "watchScheduler";
    public static final String PUSH_WATCH_SCHEDULER = "pushWatchScheduler";

    @Bean(PUSH_SCHEDULER)
    ThreadPoolTaskScheduler pushScheduler() {
        return singleThread("push-");
    }

    @Bean(HISTORY_SCHEDULER)
    ThreadPoolTaskScheduler historyScheduler() {
        return singleThread("history-");
    }

    @Bean(SUMMARY_SCHEDULER)
    ThreadPoolTaskScheduler summaryScheduler() {
        return singleThread("summary-");
    }

    @Bean(WATCH_SCHEDULER)
    ThreadPoolTaskScheduler watchScheduler() {
        return singleThread("watch-");
    }

    @Bean(PUSH_WATCH_SCHEDULER)
    ThreadPoolTaskScheduler pushWatchScheduler() {
        return singleThread("push-watch-");
    }

    private static ThreadPoolTaskScheduler singleThread(String prefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(prefix);
        scheduler.setDaemon(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
}
