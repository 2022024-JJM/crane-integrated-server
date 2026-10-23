package com.hanwha.cis.config;

import com.hanwha.cis.adapter.in.web.SlowRequestLogFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 프론트는 별도 프로젝트이므로 /api/** 에 CORS(cis.web.allowed-origins)를 연다.
 * 느린 요청 로그(cis.web.slow-request-threshold)도 /api/* 에만 건다. STOMP 핸드셰이크(/ws)와 헬스체크는 제외된다.
 */
@Configuration
public class WebConfig {

    @Bean
    WebMvcConfigurer corsConfigurer(CisProperties properties) {
        String[] origins = properties.web().allowedOrigins().toArray(String[]::new);
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**")
                        .allowedOriginPatterns(origins)
                        .allowedMethods("GET", "OPTIONS");
            }
        };
    }

    @Bean
    FilterRegistrationBean<SlowRequestLogFilter> slowRequestLogFilter(CisProperties properties) {
        FilterRegistrationBean<SlowRequestLogFilter> registration =
                new FilterRegistrationBean<>(new SlowRequestLogFilter(properties.web().slowRequestThreshold()));
        registration.addUrlPatterns("/api/*");
        return registration;
    }
}
