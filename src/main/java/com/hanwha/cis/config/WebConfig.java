package com.hanwha.cis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** 프론트는 별도 프로젝트이므로 /api/** 에 CORS(cis.web.allowed-origins)를 연다. */
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
}
