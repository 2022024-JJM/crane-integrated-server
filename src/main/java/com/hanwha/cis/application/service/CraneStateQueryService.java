package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.application.port.in.QueryCraneStateUseCase;
import com.hanwha.cis.application.port.out.CraneStateStorePort;
import com.hanwha.cis.application.support.FailureLog;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 저장소(Redis)를 먼저 읽고, 비어 있거나 장애면 메모리 상태를 돌려준다. 장애 로그는 요청마다가 아니라 시작·지속·복구만 남긴다.
 * 저장소에는 indoor·outdoor 스냅샷이 함께 있으므로 scope로 거른다.
 */
public class CraneStateQueryService implements QueryCraneStateUseCase {

    private static final Logger log = LoggerFactory.getLogger(CraneStateQueryService.class);

    private final CraneStateRegistry registry;
    private final CraneStateStorePort store;
    private final Clock clock;
    private final Duration staleThreshold;
    private final FailureLog storeFailureLog;

    public CraneStateQueryService(CraneStateRegistry registry, CraneStateStorePort store, Clock clock,
            Duration staleThreshold) {
        this.registry = registry;
        this.store = store;
        this.clock = clock;
        this.staleThreshold = staleThreshold;
        this.storeFailureLog = new FailureLog(log, "Crane state read from store (serving in-memory state)", clock,
                () -> null);
    }

    @Override
    public List<CraneSnapshot> list(CraneScope scope) {
        List<CraneSnapshot> stored = readStore(store::findAll).orElse(List.of()).stream()
                .filter(snapshot -> snapshot.scope() == scope)
                .toList();
        if (!stored.isEmpty()) {
            return stored.stream().sorted(Comparator.comparing(CraneSnapshot::craneId)).toList();
        }
        return registry.snapshots(scope, clock.instant(), staleThreshold);
    }

    @Override
    public CraneSnapshot get(CraneScope scope, String craneId) {
        return readStore(() -> store.find(craneId))
                .flatMap(found -> found)
                .or(() -> registry.find(craneId)
                        .map(state -> state.snapshot(clock.instant(), staleThreshold))
                        .filter(snapshot -> snapshot.snapshotAt() != null))
                .filter(snapshot -> snapshot.scope() == scope)
                .orElseThrow(() -> new CraneNotFoundException(craneId));
    }

    private <T> Optional<T> readStore(Supplier<T> read) {
        try {
            T value = read.get();
            storeFailureLog.success();
            return Optional.ofNullable(value);
        } catch (RuntimeException e) {
            storeFailureLog.failure(e);
            return Optional.empty();
        }
    }
}
