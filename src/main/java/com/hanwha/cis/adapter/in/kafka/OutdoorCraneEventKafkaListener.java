package com.hanwha.cis.adapter.in.kafka;

import com.hanwha.cis.application.port.in.IngestOutdoorTagUseCase;
import com.hanwha.cis.application.port.in.IngestTagReadingUseCase.RawTagEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * 외업(outdoor) 토픽을 구독해 유스케이스에 넘긴다. 레코드 키가 PLC 장비 이름 하나라 파티션 하나에만 들어오므로 스레드는 1개다.
 * 내업 리스너와 같이 메모리 상태만 만진다. 리스너 id는 스레드 이름(ingest-outdoor-0-C-1)과 client.id에 쓰인다.
 * 컨슈머 그룹은 내업 그룹 이름 뒤에 -outdoor를 붙여 따로 둔다. 한쪽의 리밸런스가 다른 쪽 수신을 멈추지 않게 하기 위함이다.
 */
@Component
public class OutdoorCraneEventKafkaListener {

    public static final String LISTENER_ID = "ingest-outdoor";

    private final IngestOutdoorTagUseCase ingest;
    private final JsonMapper json;

    public OutdoorCraneEventKafkaListener(IngestOutdoorTagUseCase ingest, JsonMapper json) {
        this.ingest = ingest;
        this.json = json;
    }

    @KafkaListener(id = LISTENER_ID, idIsGroup = false, clientIdPrefix = LISTENER_ID,
            groupId = "${spring.kafka.consumer.group-id}-outdoor",
            topics = "${cis.kafka.outdoor-topic}", concurrency = "1")
    public void onMessage(ConsumerRecord<String, String> record) {
        RawTagEvent event = KafkaRawEvent.toRawTagEvent(record, json);
        if (event == null) {
            ingest.skipUnparseable(record.key(), record.value());
            return;
        }
        ingest.ingest(event);
    }
}
