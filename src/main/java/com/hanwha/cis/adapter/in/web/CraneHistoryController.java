package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.InvalidQueryException;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryChanges;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryFrames;
import java.time.Duration;
import java.time.Instant;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 이력 조회. from/to는 ISO-8601 Instant, step은 "5s"/"1m"/"PT5S" 형식. */
@RestController
@RequestMapping("/api/cranes/{craneId}")
public class CraneHistoryController {

    private final QueryCraneHistoryUseCase query;

    public CraneHistoryController(QueryCraneHistoryUseCase query) {
        this.query = query;
    }

    @GetMapping("/history")
    public HistoryFrames history(@PathVariable String craneId,
            @RequestParam Instant from,
            @RequestParam Instant to,
            @RequestParam(required = false) String step) {
        return query.frames(craneId, from, to, parseStep(step));
    }

    @GetMapping("/changes")
    public HistoryChanges changes(@PathVariable String craneId,
            @RequestParam Instant from,
            @RequestParam Instant to,
            @RequestParam(required = false) Instant after,
            @RequestParam(required = false) Integer limit) {
        return query.changes(craneId, from, to, after, limit);
    }

    private static Duration parseStep(String step) {
        if (step == null || step.isBlank()) {
            return null;
        }
        try {
            return DurationStyle.detectAndParse(step);
        } catch (IllegalArgumentException e) {
            throw new InvalidQueryException("invalid step: " + step + " (use e.g. 1s, 5s, 1m or PT5S)");
        }
    }
}
