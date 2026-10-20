-- PostgreSQL(TimescaleDB) 전용. 테스트(H2)에서는 실행되지 않는다.

CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('crane_history', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('crane_history_diff', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE crane_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'crane_id',
    timescaledb.compress_orderby = 'time DESC'
);
ALTER TABLE crane_history_diff SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'crane_id',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('crane_history', INTERVAL '1 day');
SELECT add_compression_policy('crane_history_diff', INTERVAL '1 day');

SELECT add_retention_policy('crane_history', INTERVAL '30 days');
SELECT add_retention_policy('crane_history_diff', INTERVAL '30 days');
