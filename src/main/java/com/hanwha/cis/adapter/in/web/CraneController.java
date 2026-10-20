package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.QueryCraneStateUseCase;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 크레인 목록·현재 상태. */
@RestController
@RequestMapping("/api/cranes")
public class CraneController {

    private final QueryCraneStateUseCase query;

    public CraneController(QueryCraneStateUseCase query) {
        this.query = query;
    }

    @GetMapping
    public List<CraneSnapshot> list() {
        return query.list();
    }

    @GetMapping("/{craneId}")
    public CraneSnapshot get(@PathVariable String craneId) {
        return query.get(craneId);
    }
}
