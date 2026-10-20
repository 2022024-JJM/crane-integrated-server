package com.hanwha.cis.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * IntersysLink가 Kafka에 올리는 메시지(태그 1건). 예:
 * {"stationId":"isl-ocean-crane","craneId":"C_867","occurredAt":1792387396993,
 *  "payload":{"TagKey":"F5_1_Bay.C_867.tl_status","Value":false,"Timestamp":"2026-10-19T05:23:16.9930000Z","Quality":0}}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KafkaRawEvent(String stationId, String craneId, Long occurredAt, Payload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            @JsonProperty("TagKey") String tagKey,
            @JsonProperty("Value") Object value,
            @JsonProperty("Timestamp") String timestamp,
            @JsonProperty("Quality") Integer quality) {
    }
}
