package com.hanwha.cis.application.port.in;

/** Kafka 메시지 1건을 검증해 메모리 상태에 반영한다. */
public interface IngestTagReadingUseCase {

    IngestOutcome ingest(RawTagEvent event);

    /** JSON 파싱조차 되지 않은 메시지를 어댑터가 보고한다(카운터·샘플 로그). */
    void skipUnparseable(String recordKey, String payloadSample);

    /**
     * 어댑터가 역직렬화만 끝낸 원시 이벤트. 검증(TagKey 3단, craneId 일치, 값 정규화)은 유스케이스가 한다.
     *
     * @param recordKey  Kafka 레코드 키(없으면 null). 있으면 봉투의 craneId와 같아야 한다
     * @param occurredAt epoch ms
     * @param value      Boolean, Number 또는 String
     */
    record RawTagEvent(
            String recordKey,
            String stationId,
            String craneId,
            Long occurredAt,
            String tagKey,
            Object value,
            Integer quality) {
    }

    enum IngestOutcome {
        CHANGED, UNCHANGED, LATE_IGNORED, SKIPPED
    }
}
