package com.hanwha.cis.application.port.in;

/** 크레인별 이력 행(주기 행 + 변경 행)을 쓰기 큐에 넣는다(이력 스케줄러가 주기 호출). */
public interface RecordHistoryUseCase {

    void recordTick();
}
