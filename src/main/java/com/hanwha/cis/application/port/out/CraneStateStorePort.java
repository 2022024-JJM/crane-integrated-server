package com.hanwha.cis.application.port.out;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import java.util.Optional;

/** 최신 스냅샷 저장소(운영: Redis 해시). 기동 시 복원과 REST 조회에 쓴다. */
public interface CraneStateStorePort {

    void save(List<CraneSnapshot> snapshots);

    List<CraneSnapshot> findAll();

    Optional<CraneSnapshot> find(String craneId);
}
