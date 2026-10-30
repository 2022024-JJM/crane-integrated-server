package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.QueryCraneStateUseCase;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 내업(indoor) 크레인 목록·현재 상태. 외업은 {@link OutdoorCraneController}. */
@RestController
@RequestMapping("/api/indoor/cranes")
public class IndoorCraneController {

    private final QueryCraneStateUseCase query;

    public IndoorCraneController(QueryCraneStateUseCase query) {
        this.query = query;
    }

    @GetMapping
    public List<CraneSnapshot> list() {
        return query.list(CraneScope.INDOOR);
    }

    @GetMapping("/{craneId}")
    public CraneSnapshot get(@PathVariable String craneId) {
        return query.get(CraneScope.INDOOR, craneId);
    }
}
