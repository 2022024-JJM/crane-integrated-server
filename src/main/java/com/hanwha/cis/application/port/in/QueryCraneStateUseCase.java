package com.hanwha.cis.application.port.in;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;

/** 현재 상태 조회(REST). 저장소(Redis)를 먼저 보고 없으면 메모리를 본다. */
public interface QueryCraneStateUseCase {

    List<CraneSnapshot> list();

    /** @throws CraneNotFoundException 모르는 크레인 */
    CraneSnapshot get(String craneId);
}
