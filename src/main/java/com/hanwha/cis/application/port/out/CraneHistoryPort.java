package com.hanwha.cis.application.port.out;

import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 이력 저장(비동기 큐)과 조회. */
public interface CraneHistoryPort {

    /** crane_history 행을 쓰기 큐에 넣는다. 호출 스레드를 막지 않는다. */
    void enqueueHistory(HistoryRow row);

    /** crane_history_diff 행을 쓰기 큐에 넣는다. */
    void enqueueDiff(DiffRow row);

    /** time <= at 인 마지막 crane_history 행. */
    Optional<HistoryRow> baseline(String craneId, Instant at);

    /** fromExclusive < time <= toInclusive 인 crane_history 행을 시간순으로 최대 limit개. */
    List<HistoryRow> range(String craneId, Instant fromExclusive, Instant toInclusive, int limit);

    /** fromExclusive < time <= toInclusive 인 crane_history_diff 행을 시간순으로 최대 limit개. */
    List<DiffRow> diffs(String craneId, Instant fromExclusive, Instant toInclusive, int limit);

    record HistoryRow(Instant time, CraneSnapshot snapshot) {
    }

    record DiffRow(Instant time, boolean online, CraneSnapshot snapshot) {
    }
}
