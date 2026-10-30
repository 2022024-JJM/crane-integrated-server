package com.hanwha.cis.application.port.in;

/**
 * 어댑터가 Kafka 메시지를 역직렬화만 끝낸 원시 이벤트. 내업·외업 메시지가 같은 틀이다.
 * 검증(TagKey 형식, craneId 일치, 값 정규화)은 범위별 유스케이스가 한다.
 *
 * @param recordKey  Kafka 레코드 키(없으면 null). 있으면 봉투의 craneId와 같아야 한다
 * @param craneId    봉투의 craneId. 내업은 크레인, 외업은 PLC 장비 이름이다
 * @param occurredAt epoch ms
 * @param value      Boolean, Number, String 또는 null
 */
public record RawTagEvent(
        String recordKey,
        String stationId,
        String craneId,
        Long occurredAt,
        String tagKey,
        Object value,
        Integer quality) {
}
