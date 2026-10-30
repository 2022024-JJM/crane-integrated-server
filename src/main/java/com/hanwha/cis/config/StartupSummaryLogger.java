package com.hanwha.cis.config;

import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** 기동 완료 시 실제로 붙는 외부 시스템 주소를 한 줄로 남긴다. 배포 환경 변수가 맞게 들어갔는지 로그만으로 확인하기 위함이다. */
@Component
public class StartupSummaryLogger {

    private static final Logger log = LoggerFactory.getLogger(StartupSummaryLogger.class);

    private final Environment env;
    private final CisProperties properties;

    public StartupSummaryLogger(Environment env, CisProperties properties) {
        this.env = env;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void logTargets() {
        log.info("Ready | kafka={} topic={} outdoor-topic={} group={} concurrency={} | db={} user={} | redis={}:{}"
                        + " | logs={}",
                env.getProperty("spring.kafka.bootstrap-servers"), properties.kafka().topic(),
                properties.kafka().outdoorTopic(), env.getProperty("spring.kafka.consumer.group-id"),
                properties.kafka().concurrency(),
                withoutQuery(env.getProperty("spring.datasource.url")), env.getProperty("spring.datasource.username"),
                env.getProperty("spring.data.redis.host"), env.getProperty("spring.data.redis.port"),
                Path.of(env.getProperty("logging.file.path", "logs")).toAbsolutePath());
    }

    /** JDBC URL의 쿼리 문자열(자격 증명이 들어갈 수 있는 자리)은 남기지 않는다. */
    private static String withoutQuery(String url) {
        if (url == null) {
            return null;
        }
        int query = url.indexOf('?');
        return query < 0 ? url : url.substring(0, query);
    }
}
