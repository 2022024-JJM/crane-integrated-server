-- 모든 DB(PostgreSQL, 테스트용 H2)에서 공통으로 실행되는 스키마.
-- state 컬럼은 CraneSnapshot JSON이다. jsonb/text 대신 varchar를 쓰는 이유는 H2에서 Hibernate validate를 통과시키기 위해서다.

CREATE TABLE crane (
    crane_id      VARCHAR(64) PRIMARY KEY,
    station_id    VARCHAR(64) NOT NULL,
    bay           VARCHAR(64) NOT NULL,
    first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at  TIMESTAMP WITH TIME ZONE NOT NULL
);

-- 크레인당 1초마다 전체 상태 행(수신이 있을 때만)
CREATE TABLE crane_history (
    time     TIMESTAMP WITH TIME ZONE NOT NULL,
    crane_id VARCHAR(64) NOT NULL,
    bay      VARCHAR(64) NOT NULL,
    state    VARCHAR NOT NULL,
    PRIMARY KEY (crane_id, time)
);

-- 상태(값 또는 online)가 변했을 때만 전체 상태 행
CREATE TABLE crane_history_diff (
    time     TIMESTAMP WITH TIME ZONE NOT NULL,
    crane_id VARCHAR(64) NOT NULL,
    bay      VARCHAR(64) NOT NULL,
    online   BOOLEAN NOT NULL,
    state    VARCHAR NOT NULL,
    PRIMARY KEY (crane_id, time)
);
