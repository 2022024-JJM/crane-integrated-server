package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.QueryCraneStateUseCase;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 외업(outdoor) 크레인 목록·현재 상태. 이력은 {@link CraneHistoryController}가 같은 경로 아래에서 받는다. */
@RestController
@RequestMapping("/api/outdoor/cranes")
public class OutdoorCraneController {

    private final QueryCraneStateUseCase query;

    public OutdoorCraneController(QueryCraneStateUseCase query) {
        this.query = query;
    }

    @GetMapping
    public List<CraneSnapshot> list() {
        return query.list(CraneScope.OUTDOOR);
    }

    @GetMapping("/{craneId}")
    public CraneSnapshot get(@PathVariable String craneId) {
        return query.get(CraneScope.OUTDOOR, craneId);
    }
}
