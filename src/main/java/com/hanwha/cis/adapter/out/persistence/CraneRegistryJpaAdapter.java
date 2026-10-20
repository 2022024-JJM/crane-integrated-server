package com.hanwha.cis.adapter.out.persistence;

import com.hanwha.cis.adapter.out.persistence.HistoryWriter.CraneUpsert;
import com.hanwha.cis.application.port.out.CraneRegistryPort;
import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class CraneRegistryJpaAdapter implements CraneRegistryPort {

    private final HistoryWriter writer;
    private final CraneRepository repository;

    public CraneRegistryJpaAdapter(HistoryWriter writer, CraneRepository repository) {
        this.writer = writer;
        this.repository = repository;
    }

    @Override
    public void upsertSeen(String craneId, String stationId, String bay, Instant seenAt) {
        writer.enqueue(new CraneUpsert(craneId, stationId, bay, seenAt));
    }

    @Override
    public boolean exists(String craneId) {
        return repository.existsById(craneId);
    }
}
