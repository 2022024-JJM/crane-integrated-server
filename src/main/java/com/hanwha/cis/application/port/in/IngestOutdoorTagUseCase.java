package com.hanwha.cis.application.port.in;

/**
 * 외업(outdoor) Kafka 메시지 1건을 크레인별 메모리 상태에 반영한다. 원시 이벤트 형식은 내업과 같지만, craneId가 크레인이 아니라
 * PLC 장비 이름이고 크레인은 슬롯의 Crane_ID로 정해진다. 내업은 {@link IngestIndoorTagUseCase}.
 */
public interface IngestOutdoorTagUseCase {

    void ingest(RawTagEvent event);

    /** JSON 파싱조차 되지 않은 메시지를 어댑터가 보고한다(카운터·샘플 로그). */
    void skipUnparseable(String recordKey, String payloadSample);
}
