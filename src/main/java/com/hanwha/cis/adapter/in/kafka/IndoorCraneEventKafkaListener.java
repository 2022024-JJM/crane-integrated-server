package com.hanwha.cis.adapter.in.kafka;

import com.hanwha.cis.application.port.in.IngestIndoorTagUseCase;
import com.hanwha.cis.application.port.in.RawTagEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * 내업(indoor) 토픽을 구독해 JSON을 {@link KafkaRawEvent}로 읽고 유스케이스에 넘긴다. 파티션(=크레인)당 스레드 1개.
 * 이 스레드는 메모리 상태만 만지고 DB·Redis는 호출하지 않는다. 예외는 삼키지 않고 컨테이너 에러 핸들러에 맡긴다.
 * 리스너 id는 스레드 이름(ingest-indoor-0-C-1)과 Kafka client.id에 쓰인다. 컨슈머 그룹은 기본 그룹 이름 뒤에 -indoor를 붙인다.
 * 외업은 {@link OutdoorCraneEventKafkaListener}.
 */
@Component
public class IndoorCraneEventKafkaListener {

    public static final String LISTENER_ID = "ingest-indoor";

    private final IngestIndoorTagUseCase ingest;
    private final JsonMapper json;

    public IndoorCraneEventKafkaListener(IngestIndoorTagUseCase ingest, JsonMapper json) {
        this.ingest = ingest;
        this.json = json;
    }

    @KafkaListener(id = LISTENER_ID, idIsGroup = false, clientIdPrefix = LISTENER_ID,
            groupId = "${spring.kafka.consumer.group-id}-indoor",
            topics = "${cis.kafka.indoor.topic}", concurrency = "${cis.kafka.indoor.concurrency}")
    public void onMessage(ConsumerRecord<String, String> record) {
        RawTagEvent event = KafkaRawEvent.toRawTagEvent(record, json);
        if (event == null) {
            ingest.skipUnparseable(record.key(), record.value());
            return;
        }
        ingest.ingest(event);
    }
}
