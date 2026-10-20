package com.hanwha.cis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 리스너 예외는 재시도 없이(FixedBackOff 0회) 로그 후 다음 레코드로 넘어간다.
 * 검증 실패는 유스케이스가 건너뛰므로 여기까지 오는 예외는 버그다. Boot 자동 구성이 이 빈을 컨테이너 팩토리에 끼운다.
 */
@Configuration
public class KafkaConfig {

    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(0L, 0L));
    }
}
