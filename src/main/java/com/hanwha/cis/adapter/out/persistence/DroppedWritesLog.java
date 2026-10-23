package com.hanwha.cis.adapter.out.persistence;

import com.hanwha.cis.adapter.out.persistence.HistoryWriter.WriteOp;
import com.hanwha.cis.application.support.Durations;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * 버린 이력 쓰기를 원인별 건수와 행 time 범위로 모아 ERROR로 남긴다. 리플레이에서 비는 구간을 로그로 찾기 위함이다
 * (큐가 버퍼링하므로 장애 시작·복구 시각이 DB의 빈 구간과 같지 않다).
 * 버리기 시작하면 바로 한 줄, 이어지면 1분마다 그 사이 버린 것을 한 줄, 1분 동안 버린 것이 없으면 INFO로 끝(총 건수·기간)을 남긴다.
 * {@link #record}는 여러 스레드에서, {@link #report}·{@link #flush}는 쓰기 스레드와 종료 스레드에서 부른다.
 */
final class DroppedWritesLog {

    enum Reason {
        WRITE_FAILED("failed to write"), QUEUE_FULL("rejected by full queue"), SHUTDOWN("unwritten at shutdown");

        private final String text;

        Reason(String text) {
            this.text = text;
        }
    }

    static final Duration REPORT_EVERY = Duration.ofSeconds(60);

    private final Logger log;
    private final Clock clock;
    private final Supplier<String> detail;

    private final Map<Reason, Range> pending = new EnumMap<>(Reason.class);
    private Instant episodeStart;
    private Instant lastDropAt;
    private Instant lastReportAt;
    private long episodeTotal;

    /** detail은 유실 줄 끝에 [ ]로 붙는다. */
    DroppedWritesLog(Logger log, Clock clock, Supplier<String> detail) {
        this.log = log;
        this.clock = clock;
        this.detail = detail;
    }

    synchronized void record(Reason reason, Collection<? extends WriteOp> ops) {
        if (ops.isEmpty()) {
            return;
        }
        Range range = pending.computeIfAbsent(reason, r -> new Range());
        for (WriteOp op : ops) {
            range.add(op.time());
        }
        lastDropAt = clock.instant();
        if (episodeStart == null) {
            episodeStart = lastDropAt;
        }
    }

    /** 남길 때가 됐으면 남긴다. 쓰기 스레드가 루프마다 부른다. */
    synchronized void report() {
        Instant now = clock.instant();
        if (!pending.isEmpty()) {
            if (lastReportAt == null || !now.isBefore(lastReportAt.plus(REPORT_EVERY))) {
                logPending(now);
            }
            return;
        }
        if (episodeStart != null && !now.isBefore(lastDropAt.plus(REPORT_EVERY))) {
            log.info("History writes no longer dropped: {} write(s) dropped in total over {}", episodeTotal,
                    Durations.format(Duration.between(episodeStart, lastDropAt)));
            episodeStart = null;
            lastDropAt = null;
            lastReportAt = null;
            episodeTotal = 0;
        }
    }

    /** 모인 것을 바로 남긴다(종료 시). */
    synchronized void flush() {
        if (!pending.isEmpty()) {
            logPending(clock.instant());
        }
    }

    private void logPending(Instant now) {
        StringJoiner parts = new StringJoiner(", ");
        for (Map.Entry<Reason, Range> entry : pending.entrySet()) {
            Range range = entry.getValue();
            parts.add(range.count + " " + entry.getKey().text + " (time " + range + ")");
            episodeTotal += range.count;
        }
        log.error("History writes dropped: {} [{}]", parts, detail.get());
        pending.clear();
        lastReportAt = now;
    }

    private static final class Range {
        private long count;
        private Instant min;
        private Instant max;

        void add(Instant time) {
            count++;
            if (time == null) {
                return;
            }
            if (min == null || time.isBefore(min)) {
                min = time;
            }
            if (max == null || time.isAfter(max)) {
                max = time;
            }
        }

        @Override
        public String toString() {
            if (min == null) {
                return "unknown";
            }
            return min.equals(max) ? min.toString() : min + " ~ " + max;
        }
    }
}
