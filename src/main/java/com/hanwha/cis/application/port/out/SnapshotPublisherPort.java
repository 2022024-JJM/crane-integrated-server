package com.hanwha.cis.application.port.out;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;

/** 변한 스냅샷 묶음을 푸시 경로(운영: Redis 채널 → STOMP)에 발행한다. */
public interface SnapshotPublisherPort {

    void publish(List<CraneSnapshot> snapshots);
}
