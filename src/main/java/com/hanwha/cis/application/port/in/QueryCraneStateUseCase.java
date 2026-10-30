package com.hanwha.cis.application.port.in;

import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;

/** 현재 상태 조회(REST). 저장소(Redis)를 먼저 보고 없으면 메모리를 본다. scope(indoor/outdoor)별로 나눠 돌려준다. */
public interface QueryCraneStateUseCase {

    List<CraneSnapshot> list(CraneScope scope);

    /** @throws CraneNotFoundException 모르는 크레인이거나 다른 scope의 크레인 */
    CraneSnapshot get(CraneScope scope, String craneId);
}
