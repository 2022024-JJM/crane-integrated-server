package com.hanwha.cis.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/** 이 서버의 모든 설정(cis.*). 기본값은 각 컴포넌트에 선언한다. */
@ConfigurationProperties("cis")
@Validated
public record CisProperties(
        @Valid @NotNull @DefaultValue Kafka kafka,
        @Valid @NotNull @DefaultValue Push push,
        @Valid @NotNull @DefaultValue History history,
        @Valid @NotNull @DefaultValue State state,
        @Valid @NotNull @DefaultValue Query query,
        @Valid @NotNull @DefaultValue Redis redis,
        @Valid @NotNull @DefaultValue Web web) {

    public record Kafka(
            @NotBlank @DefaultValue("ocean.crane.indoor.event") String topic,
            @Min(1) @DefaultValue("3") int concurrency) {
    }

    public record Push(
            @NotNull @DurationMin(millis = 10) @DefaultValue("250ms") Duration interval) {
    }

    public record History(
            @NotNull @DurationMin(millis = 10) @DefaultValue("1s") Duration interval,
            @Min(10) @DefaultValue("10000") int queueCapacity,
            @Min(1) @DefaultValue("500") int batchSize,
            @NotNull @DurationMin(millis = 10) @DefaultValue("1s") Duration flushInterval,
            @Min(0) @DefaultValue("3") int maxRetries) {
    }

    public record State(
            @NotNull @DurationMin(seconds = 1) @DefaultValue("30s") Duration staleThreshold) {
    }

    public record Query(
            @NotNull @DurationMin(seconds = 1) @DefaultValue("1h") Duration maxRange,
            @NotNull @DurationMin(seconds = 1) @DefaultValue("1s") Duration defaultStep,
            @Min(1) @DefaultValue("5000") int maxPoints,
            @Min(1) @DefaultValue("500") int defaultChangeLimit,
            @Min(1) @DefaultValue("5000") int maxChangeLimit) {
    }

    public record Redis(
            @NotBlank @DefaultValue("cis:crane:state") String stateKey,
            @NotBlank @DefaultValue("cis:crane:snapshots") String snapshotChannel) {
    }

    public record Web(
            @NotEmpty @DefaultValue("http://localhost:*") List<String> allowedOrigins) {
    }
}
