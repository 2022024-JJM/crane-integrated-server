package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.application.port.in.InvalidQueryException;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase;
import com.hanwha.cis.application.port.out.CraneHistoryPort;
import com.hanwha.cis.application.port.out.CraneHistoryPort.DiffRow;
import com.hanwha.cis.application.port.out.CraneHistoryPort.HistoryRow;
import com.hanwha.cis.application.port.out.CraneRegistryPort;
import com.hanwha.cis.domain.model.TagValue;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

/** baseline + sample-and-hold 프레임 생성, diff 커서 페이징, 조회 상한 검증. */
public class CraneHistoryQueryService implements QueryCraneHistoryUseCase {

    /**
     * @param maxRange           to - from 상한
     * @param defaultStep        step 생략 시 값
     * @param maxPoints          프레임 수 상한(조회 행 수 상한으로도 쓴다)
     * @param defaultChangeLimit changes limit 생략 시 값
     * @param maxChangeLimit     changes limit 상한
     */
    public record Limits(Duration maxRange, Duration defaultStep, int maxPoints, int defaultChangeLimit,
            int maxChangeLimit) {
    }

    private static final Duration MIN_STEP = Duration.ofSeconds(1);

    private final CraneHistoryPort history;
    private final CraneRegistryPort cranes;
    private final CraneStateRegistry registry;
    private final Clock clock;
    private final Limits limits;

    public CraneHistoryQueryService(CraneHistoryPort history, CraneRegistryPort cranes,
            CraneStateRegistry registry, Clock clock, Limits limits) {
        this.history = history;
        this.cranes = cranes;
        this.registry = registry;
        this.clock = clock;
        this.limits = limits;
    }

    @Override
    public HistoryFrames frames(String craneId, Instant from, Instant to, Duration step) {
        requireKnownCrane(craneId);
        Instant now = clock.instant();
        validateRange(from, to, now);
        Duration effectiveStep = step == null ? limits.defaultStep() : step;
        if (effectiveStep.compareTo(MIN_STEP) < 0) {
            throw new InvalidQueryException("step must be at least " + MIN_STEP);
        }
        // 미래 구간은 프레임을 만들지 않는다: now 이하의 마지막 step 격자 시각에서 끊는다.
        Instant end = to.isAfter(now)
                ? from.plus(effectiveStep.multipliedBy(Duration.between(from, now).dividedBy(effectiveStep)))
                : to;
        long points = Duration.between(from, end).toMillis() / effectiveStep.toMillis() + 1;
        if (points > limits.maxPoints()) {
            throw new InvalidQueryException(
                    "too many points: " + points + " > " + limits.maxPoints() + " (use a larger step)");
        }

        Optional<HistoryRow> baseline = history.baseline(craneId, from);
        List<HistoryRow> rows = history.range(craneId, from, end, limits.maxPoints() + 1);
        boolean truncated = rows.size() > limits.maxPoints();
        if (truncated) {
            rows = rows.subList(0, limits.maxPoints());
        }
        Instant lastFrameAt = truncated ? rows.getLast().time() : end;

        TreeSet<String> tags = new TreeSet<>();
        baseline.ifPresent(row -> tags.addAll(row.snapshot().tags().keySet()));
        rows.forEach(row -> tags.addAll(row.snapshot().tags().keySet()));

        Map<String, TagValue> current = baseline.map(row -> row.snapshot().values()).orElse(Map.of());
        List<Frame> frames = new ArrayList<>();
        int next = 0;
        for (Instant cursor = from; !cursor.isAfter(lastFrameAt); cursor = cursor.plus(effectiveStep)) {
            while (next < rows.size() && !rows.get(next).time().isAfter(cursor)) {
                current = rows.get(next).snapshot().values();
                next++;
            }
            frames.add(new Frame(cursor, current));
        }
        Frame baselineFrame = baseline.map(row -> new Frame(row.time(), row.snapshot().values())).orElse(null);
        return new HistoryFrames(craneId, from, end, effectiveStep, List.copyOf(tags), baselineFrame, frames,
                truncated);
    }

    @Override
    public HistoryChanges changes(String craneId, Instant from, Instant to, Instant after, Integer limit) {
        requireKnownCrane(craneId);
        Instant now = clock.instant();
        validateRange(from, to, now);
        int effectiveLimit = limit == null ? limits.defaultChangeLimit() : limit;
        if (effectiveLimit < 1 || effectiveLimit > limits.maxChangeLimit()) {
            throw new InvalidQueryException("limit must be between 1 and " + limits.maxChangeLimit());
        }
        // 첫 페이지는 from을 포함해야 하므로 1ms 앞을 exclusive 하한으로 쓴다(시각은 ms 정밀도).
        Instant lowerExclusive = after != null && after.isAfter(from) ? after : from.minusMillis(1);

        Instant end = to.isAfter(now) ? now : to;

        List<DiffRow> rows = history.diffs(craneId, lowerExclusive, end, effectiveLimit + 1);
        boolean more = rows.size() > effectiveLimit;
        if (more) {
            rows = rows.subList(0, effectiveLimit);
        }
        List<Change> items = rows.stream()
                .map(row -> new Change(row.time(), row.online(), row.snapshot().values()))
                .toList();
        Instant nextCursor = more ? rows.getLast().time() : null;
        return new HistoryChanges(items, nextCursor);
    }

    private void requireKnownCrane(String craneId) {
        if (registry.find(craneId).isEmpty() && !cranes.exists(craneId)) {
            throw new CraneNotFoundException(craneId);
        }
    }

    private void validateRange(Instant from, Instant to, Instant now) {
        if (from == null || to == null) {
            throw new InvalidQueryException("from and to are required");
        }
        if (!from.isBefore(to)) {
            throw new InvalidQueryException("from must be before to");
        }
        if (!from.isBefore(now)) {
            throw new InvalidQueryException("from must be in the past");
        }
        if (Duration.between(from, to).compareTo(limits.maxRange()) > 0) {
            throw new InvalidQueryException("range must not exceed " + limits.maxRange());
        }
    }
}
