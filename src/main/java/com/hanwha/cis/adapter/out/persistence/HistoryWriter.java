package com.hanwha.cis.adapter.out.persistence;

import com.hanwha.cis.application.support.FailureLog;
import com.hanwha.cis.config.CisProperties;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * DB 쓰기 전용 스레드. bounded 큐를 배치로 비우고(배치 크기 또는 flush 간격), 실패하면 재시도 후 버린다.
 * 실패 로그는 장애 시작·1분마다 지속·복구만 남기고(큐 길이·버린 건수 포함), 재시도 한 번 한 번은 DEBUG다.
 * Kafka·스케줄러 스레드는 {@link #enqueue}만 호출하므로 DB 장애가 수집·푸시를 막지 않는다.
 * INSERT는 ON CONFLICT DO NOTHING이라 재시작·재전송으로 같은 (crane_id, time)이 다시 와도 배치가 실패하지 않는다.
 */
@Component
public class HistoryWriter implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(HistoryWriter.class);

    /** 스케줄러보다 먼저 시작하고 나중에 멈춘다. */
    public static final int PHASE = 100;

    private static final String INSERT_HISTORY = """
            INSERT INTO crane_history (time, crane_id, bay, state) VALUES (?, ?, ?, ?)
            ON CONFLICT DO NOTHING""";
    private static final String INSERT_DIFF = """
            INSERT INTO crane_history_diff (time, crane_id, bay, online, state) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING""";
    private static final String MERGE_CRANE = """
            MERGE INTO crane t
            USING (VALUES (CAST(? AS VARCHAR(64)), CAST(? AS VARCHAR(64)), CAST(? AS VARCHAR(64)),
                           CAST(? AS TIMESTAMP WITH TIME ZONE))) AS s(crane_id, station_id, bay, seen_at)
            ON t.crane_id = s.crane_id
            WHEN MATCHED THEN UPDATE SET station_id = s.station_id, bay = s.bay,
                                         last_seen_at = GREATEST(t.last_seen_at, s.seen_at)
            WHEN NOT MATCHED THEN INSERT (crane_id, station_id, bay, first_seen_at, last_seen_at)
                                  VALUES (s.crane_id, s.station_id, s.bay, s.seen_at, s.seen_at)""";

    public sealed interface WriteOp permits HistoryInsert, DiffInsert, CraneUpsert {
    }

    public record HistoryInsert(Instant time, String craneId, String bay, String stateJson) implements WriteOp {
    }

    public record DiffInsert(Instant time, String craneId, String bay, boolean online, String stateJson)
            implements WriteOp {
    }

    public record CraneUpsert(String craneId, String stationId, String bay, Instant seenAt) implements WriteOp {
    }

    private final BlockingQueue<WriteOp> queue;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final int batchSize;
    private final Duration flushInterval;
    private final int maxRetries;

    private final LongAdder historyRows = new LongAdder();
    private final LongAdder diffRows = new LongAdder();
    private final LongAdder craneUpserts = new LongAdder();
    private final LongAdder writeFailures = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final AtomicLong lastDropLogAt = new AtomicLong();
    private final FailureLog writeFailureLog;

    private volatile boolean running;
    private Thread thread;

    public HistoryWriter(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
            CisProperties properties) {
        CisProperties.History history = properties.history();
        this.queue = new ArrayBlockingQueue<>(history.queueCapacity());
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.batchSize = history.batchSize();
        this.flushInterval = history.flushInterval();
        this.maxRetries = history.maxRetries();
        this.writeFailureLog = new FailureLog(log, "History write to DB", Clock.systemUTC(),
                () -> "queue=" + queue.size() + "/" + history.queueCapacity() + ", dropped=" + dropped.sum()
                        + " total");
    }

    /** 큐가 가득 차면 버리고 false. 호출 스레드를 막지 않는다. */
    public boolean enqueue(WriteOp op) {
        if (queue.offer(op)) {
            return true;
        }
        dropped.increment();
        long now = System.currentTimeMillis();
        long last = lastDropLogAt.get();
        if (now - last > 60_000 && lastDropLogAt.compareAndSet(last, now)) {
            log.warn("History write queue is full ({}), dropping writes (dropped so far: {})",
                    queue.size(), dropped.sum());
        }
        return false;
    }

    public int queueSize() {
        return queue.size();
    }

    public long historyRows() {
        return historyRows.sum();
    }

    public long diffRows() {
        return diffRows.sum();
    }

    public long craneUpserts() {
        return craneUpserts.sum();
    }

    public long writeFailures() {
        return writeFailures.sum();
    }

    public long dropped() {
        return dropped.sum();
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::runLoop, "history-writer");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void stop() {
        running = false;
        Thread t = thread;
        if (t == null) {
            return;
        }
        try {
            t.join(flushInterval.toMillis() + 5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!queue.isEmpty()) {
            log.warn("Stopped with {} unwritten history write(s)", queue.size());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private void runLoop() {
        List<WriteOp> batch = new ArrayList<>(batchSize);
        while (running || !queue.isEmpty()) {
            try {
                WriteOp first = queue.poll(flushInterval.toMillis(), TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                long deadline = System.nanoTime() + flushInterval.toNanos();
                while (batch.size() < batchSize) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        break;
                    }
                    WriteOp next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }
                flushWithRetry(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                dropped.add(batch.size());
                writeFailureLog.failure(e);
            } finally {
                batch.clear();
            }
        }
    }

    private void flushWithRetry(List<WriteOp> batch) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                transaction.executeWithoutResult(status -> flush(batch));
                writeFailureLog.success();
                return;
            } catch (DataAccessException | TransactionException e) {
                writeFailures.increment();
                if (attempt > maxRetries || !running) {
                    dropped.add(batch.size());
                    writeFailureLog.failure(e);
                    log.debug("Dropped {} history write(s) after {} attempt(s)", batch.size(), attempt);
                    return;
                }
                writeFailureLog.failure(e);
                long backoffMillis = Math.min(1_000L << (attempt - 1), 10_000L);
                log.debug("History write failed (attempt {}/{}), retrying in {} ms", attempt, maxRetries + 1,
                        backoffMillis);
                Thread.sleep(backoffMillis);
            }
        }
    }

    private void flush(List<WriteOp> batch) {
        List<HistoryInsert> history = new ArrayList<>();
        List<DiffInsert> diffs = new ArrayList<>();
        Map<String, CraneUpsert> cranes = new LinkedHashMap<>();
        for (WriteOp op : batch) {
            switch (op) {
                case HistoryInsert h -> history.add(h);
                case DiffInsert d -> diffs.add(d);
                case CraneUpsert c -> cranes.put(c.craneId(), c);
            }
        }
        if (!history.isEmpty()) {
            jdbc.batchUpdate(INSERT_HISTORY, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    HistoryInsert row = history.get(i);
                    ps.setObject(1, utc(row.time()));
                    ps.setString(2, row.craneId());
                    ps.setString(3, row.bay());
                    ps.setString(4, row.stateJson());
                }

                @Override
                public int getBatchSize() {
                    return history.size();
                }
            });
            historyRows.add(history.size());
        }
        if (!diffs.isEmpty()) {
            jdbc.batchUpdate(INSERT_DIFF, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    DiffInsert row = diffs.get(i);
                    ps.setObject(1, utc(row.time()));
                    ps.setString(2, row.craneId());
                    ps.setString(3, row.bay());
                    ps.setBoolean(4, row.online());
                    ps.setString(5, row.stateJson());
                }

                @Override
                public int getBatchSize() {
                    return diffs.size();
                }
            });
            diffRows.add(diffs.size());
        }
        if (!cranes.isEmpty()) {
            List<CraneUpsert> upserts = new ArrayList<>(cranes.values());
            jdbc.batchUpdate(MERGE_CRANE, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    CraneUpsert row = upserts.get(i);
                    ps.setString(1, row.craneId());
                    ps.setString(2, row.stationId());
                    ps.setString(3, row.bay());
                    ps.setObject(4, utc(row.seenAt()));
                }

                @Override
                public int getBatchSize() {
                    return upserts.size();
                }
            });
            craneUpserts.add(upserts.size());
        }
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
