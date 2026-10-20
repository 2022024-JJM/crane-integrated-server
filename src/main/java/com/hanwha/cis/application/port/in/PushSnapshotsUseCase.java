package com.hanwha.cis.application.port.in;

/** 변한 크레인의 스냅샷을 저장소에 쓰고 구독자에게 발행한다(푸시 스케줄러가 주기 호출). */
public interface PushSnapshotsUseCase {

    /** @return 발행한 스냅샷 수 */
    int pushDirty();
}
