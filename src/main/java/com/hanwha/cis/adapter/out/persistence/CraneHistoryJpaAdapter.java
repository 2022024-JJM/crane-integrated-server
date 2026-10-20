package com.hanwha.cis.adapter.out.persistence;

import com.hanwha.cis.adapter.out.persistence.HistoryWriter.DiffInsert;
import com.hanwha.cis.adapter.out.persistence.HistoryWriter.HistoryInsert;
import com.hanwha.cis.application.port.out.CraneHistoryPort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** 쓰기는 {@link HistoryWriter} 큐로, 읽기는 JPA 리포지토리로. state JSON ↔ CraneSnapshot 변환을 담당한다. */
@Component
public class CraneHistoryJpaAdapter implements CraneHistoryPort {

    private final HistoryWriter writer;
    private final CraneHistoryRepository historyRepository;
    private final CraneHistoryDiffRepository diffRepository;
    private final JsonMapper json;

    public CraneHistoryJpaAdapter(HistoryWriter writer, CraneHistoryRepository historyRepository,
            CraneHistoryDiffRepository diffRepository, JsonMapper json) {
        this.writer = writer;
        this.historyRepository = historyRepository;
        this.diffRepository = diffRepository;
        this.json = json;
    }

    @Override
    public void enqueueHistory(HistoryRow row) {
        CraneSnapshot snapshot = row.snapshot();
        writer.enqueue(new HistoryInsert(row.time(), snapshot.craneId(), snapshot.bay(),
                json.writeValueAsString(snapshot)));
    }

    @Override
    public void enqueueDiff(DiffRow row) {
        CraneSnapshot snapshot = row.snapshot();
        writer.enqueue(new DiffInsert(row.time(), snapshot.craneId(), snapshot.bay(), row.online(),
                json.writeValueAsString(snapshot)));
    }

    @Override
    public Optional<HistoryRow> baseline(String craneId, Instant at) {
        return historyRepository.findLatestAtOrBefore(craneId, at, PageRequest.of(0, 1)).stream()
                .findFirst()
                .map(this::toRow);
    }

    @Override
    public List<HistoryRow> range(String craneId, Instant fromExclusive, Instant toInclusive, int limit) {
        return historyRepository.findInRange(craneId, fromExclusive, toInclusive, PageRequest.of(0, limit))
                .stream()
                .map(this::toRow)
                .toList();
    }

    @Override
    public List<DiffRow> diffs(String craneId, Instant fromExclusive, Instant toInclusive, int limit) {
        return diffRepository.findInRange(craneId, fromExclusive, toInclusive, PageRequest.of(0, limit))
                .stream()
                .map(e -> new DiffRow(e.getId().getTime(), e.isOnline(), parse(e.getState())))
                .toList();
    }

    private HistoryRow toRow(CraneHistoryEntity entity) {
        return new HistoryRow(entity.getId().getTime(), parse(entity.getState()));
    }

    private CraneSnapshot parse(String state) {
        return json.readValue(state, CraneSnapshot.class);
    }
}
