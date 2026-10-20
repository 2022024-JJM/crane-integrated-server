package com.hanwha.cis.support;

import com.hanwha.cis.application.port.out.CraneHistoryPort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/** DB를 대신하는 메모리 이력 저장소. 포트 계약(exclusive/inclusive 경계, 정렬, limit)을 그대로 구현한다. */
public class InMemoryCraneHistoryPort implements CraneHistoryPort {

    private final List<HistoryRow> history = new CopyOnWriteArrayList<>();
    private final List<DiffRow> diffs = new CopyOnWriteArrayList<>();

    @Override
    public void enqueueHistory(HistoryRow row) {
        history.add(row);
    }

    @Override
    public void enqueueDiff(DiffRow row) {
        diffs.add(row);
    }

    @Override
    public Optional<HistoryRow> baseline(String craneId, Instant at) {
        return history.stream()
                .filter(r -> r.snapshot().craneId().equals(craneId) && !r.time().isAfter(at))
                .max(Comparator.comparing(HistoryRow::time));
    }

    @Override
    public List<HistoryRow> range(String craneId, Instant fromExclusive, Instant toInclusive, int limit) {
        return history.stream()
                .filter(r -> r.snapshot().craneId().equals(craneId))
                .filter(r -> r.time().isAfter(fromExclusive) && !r.time().isAfter(toInclusive))
                .sorted(Comparator.comparing(HistoryRow::time))
                .limit(limit)
                .toList();
    }

    @Override
    public List<DiffRow> diffs(String craneId, Instant fromExclusive, Instant toInclusive, int limit) {
        return diffs.stream()
                .filter(r -> r.snapshot().craneId().equals(craneId))
                .filter(r -> r.time().isAfter(fromExclusive) && !r.time().isAfter(toInclusive))
                .sorted(Comparator.comparing(DiffRow::time))
                .limit(limit)
                .toList();
    }

    public List<HistoryRow> historyRows() {
        return history;
    }

    public List<DiffRow> diffRows() {
        return diffs;
    }

    public void addHistory(Instant time, CraneSnapshot snapshot) {
        history.add(new HistoryRow(time, snapshot));
    }

    public void addDiff(Instant time, CraneSnapshot snapshot) {
        diffs.add(new DiffRow(time, snapshot.online(), snapshot));
    }
}
