package com.hanwha.cis.application.port.in;

/** 내업(indoor) Kafka 메시지 1건을 검증해 메모리 상태에 반영한다. 외업은 {@link IngestOutdoorTagUseCase}. */
public interface IngestIndoorTagUseCase {

    IngestOutcome ingest(RawTagEvent event);

    /** JSON 파싱조차 되지 않은 메시지를 어댑터가 보고한다(카운터·샘플 로그). */
    void skipUnparseable(String recordKey, String payloadSample);

    enum IngestOutcome {
        CHANGED, UNCHANGED, LATE_IGNORED, SKIPPED
    }
}
