package com.hanwha.cis.adapter.in.web;

import com.hanwha.cis.application.port.in.InvalidQueryException;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryChanges;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryFrames;
import com.hanwha.cis.application.support.FailureLog;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 이력 조회. from/to는 ISO-8601 Instant, step은 "5s"/"1m"/"PT5S" 형식.
 * 내업·외업이 같은 테이블이고 크레인 ID가 겹치지 않으므로, 두 경로 모두 크레인 ID로만 찾는다(scope는 보지 않는다).
 * DB 장애는 요청마다 스택트레이스를 남기지 않고 시작·지속·복구만 남기며(FailureLog) 503으로 돌려준다.
 */
@RestController
@RequestMapping({"/api/cranes/{craneId}", "/api/outdoor/cranes/{craneId}"})
public class CraneHistoryController {

    private static final Logger log = LoggerFactory.getLogger(CraneHistoryController.class);

    private final QueryCraneHistoryUseCase query;
    private final FailureLog dbFailureLog = new FailureLog(log, "History query from DB");

    public CraneHistoryController(QueryCraneHistoryUseCase query) {
        this.query = query;
    }

    @GetMapping("/history")
    public HistoryFrames history(@PathVariable String craneId,
            @RequestParam Instant from,
            @RequestParam Instant to,
            @RequestParam(required = false) String step) {
        Duration parsedStep = parseStep(step);
        return fromDb(() -> query.frames(craneId, from, to, parsedStep));
    }

    @GetMapping("/changes")
    public HistoryChanges changes(@PathVariable String craneId,
            @RequestParam Instant from,
            @RequestParam Instant to,
            @RequestParam(required = false) Instant after,
            @RequestParam(required = false) Integer limit) {
        return fromDb(() -> query.changes(craneId, from, to, after, limit));
    }

    private <T> T fromDb(Supplier<T> call) {
        try {
            T result = call.get();
            dbFailureLog.success();
            return result;
        } catch (DataAccessException | TransactionException e) {
            dbFailureLog.failure(e);
            throw new HistoryUnavailableException(e);
        }
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
