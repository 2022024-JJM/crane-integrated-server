package com.hanwha.cis.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hanwha.cis.application.port.in.RawTagEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * IntersysLink가 Kafka에 올리는 메시지(태그 1건). 내업·외업이 같은 틀이다. 예:
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

    /** 레코드를 원시 이벤트로 읽는다. 값이 없거나 JSON이 아니거나 payload가 없으면 null(유스케이스의 skipUnparseable 대상). */
    static RawTagEvent toRawTagEvent(ConsumerRecord<String, String> record, JsonMapper json) {
        if (record.value() == null) {
            return null;
        }
        KafkaRawEvent event;
        try {
            event = json.readValue(record.value(), KafkaRawEvent.class);
        } catch (JacksonException e) {
            return null;
        }
        if (event == null || event.payload() == null) {
            return null;
        }
        Payload p = event.payload();
        return new RawTagEvent(record.key(), event.stationId(), event.craneId(), event.occurredAt(),
                p.tagKey(), p.value(), p.quality());
    }
}
