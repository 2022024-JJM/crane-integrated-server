# crane-integrated-server

크레인 설비 데이터를 Kafka에서 받아 가공한 뒤, 웹 모니터링 화면에 실시간으로 전달하고 이력을 저장하는 Spring Boot 서버입니다.

## 시스템 구성

```mermaid
flowchart LR
    subgraph SRC["① 데이터 소스"]
        NR["Node-RED<br/>크레인 시뮬레이터"]
    end
    subgraph ISL["② IntersysLink v4"]
        MA["MQTT Agent"]
        ENG["Engine"]
        KP["Kafka Provider<br/>(Producer)"]
    end
    KAFKA[("③ Apache Kafka<br/>Broker / Topic")]
    subgraph SRV["④ crane-integrated-server"]
        KC["Kafka Consumer"]
        SVC["Service<br/>(데이터 가공/처리)"]
        API["WebSocket / REST API"]
    end
    WEB["⑤ 웹 모니터링 화면<br/>(Browser)"]
    DB[("⑥ PostgreSQL<br/>(TimescaleDB)")]
    REDIS[("Redis")]

    NR -- MQTT Publish --> MA
    MA --> ENG
    ENG --> KP
    KP -- Produce --> KAFKA
    KAFKA -- Subscribe --> KC
    KC --> SVC
    SVC -- 최신 상태 / Pub-Sub --> REDIS
    REDIS --> API
    SVC -- 이력 저장 / 조회 --> DB
    API -- "WebSocket(STOMP) / HTTP" --> WEB
```

이 저장소는 ④에 해당합니다. 크레인 값은 Node-RED 크레인 시뮬레이터가 만들어 MQTT로 발행하고, 수집과 Kafka 적재는 IntersysLink v4의 MQTT Agent가 담당하며, 이 서버는 토픽을 구독만 합니다.

서버 안의 흐름은 다음과 같습니다.

```
Kafka ─▶ KafkaListener ─▶ TagIngestionService ─▶ 메모리 상태(크레인별, 변경 감지)
                                                       │ 변한 크레인
   250ms ─▶ SnapshotPushService ──▶ Redis HASH cis:crane:state ──▶ REST 조회
                                 └▶ Redis PUBLISH cis:crane:snapshots ─▶ 구독 ─▶ STOMP /topic/cranes, /topic/cranes/{id}
   1s    ─▶ HistoryRecordingService ─▶ HistoryWriter 큐 ─▶ 배치 INSERT crane_history / crane_history_diff
```

- 메시지는 태그 1개 단위 JSON입니다. `TagKey`는 `{bay}.{craneId}.{tagName}`, `Value`는 boolean 또는 숫자입니다.
- 값이 바뀐 크레인만 250ms 주기로 Redis에 저장·발행하고, WebSocket은 Redis 채널을 구독해 브라우저로 밀어 줍니다.
- 30초 동안 수신이 없는 크레인은 `online=false`로 표시합니다.
- 이력은 크레인당 1초마다 전체 상태 행(`crane_history`)과, 상태가 바뀐 시점의 전체 상태 행(`crane_history_diff`)을 TimescaleDB 하이퍼테이블에 저장합니다(30일 보존, 1일 경과분 압축).

## 코드 구조 (헥사고날)

```
com.hanwha.cis
├── domain.model      TagKey, TagValue, TagReading, TagState, CraneState, CraneSnapshot  (프레임워크 비의존)
├── application
│   ├── port.in       IngestTagReading / PushSnapshots / RecordHistory / QueryCraneState / QueryCraneHistory UseCase
│   ├── port.out      CraneStateStorePort(Redis), SnapshotPublisherPort(Redis 채널), CraneHistoryPort(DB), CraneRegistryPort(DB)
│   └── service       CraneStateRegistry, TagIngestionService, SnapshotPushService, HistoryRecordingService, 조회 서비스
├── adapter
│   ├── in.kafka      CraneEventKafkaListener
│   ├── in.scheduler  SnapshotPushScheduler(250ms), HistoryRecordScheduler(1s), SummaryLogScheduler(60s)
│   ├── in.web        CraneController, CraneHistoryController, ApiExceptionHandler(ProblemDetail)
│   ├── in.websocket  StompConfig, SnapshotStompBroadcaster, RedisSnapshotSubscriber
│   ├── out.redis     RedisCraneStateStore, RedisSnapshotPublisher
│   └── out.persistence HistoryWriter(전용 쓰기 스레드), JPA 엔티티·리포지토리, 어댑터
└── config            CisProperties(cis.*), ApplicationConfig, SchedulingConfig, KafkaConfig, RedisConfig, WebConfig, MetricsConfig
```

의존 방향(domain ← application ← adapter/config, 어댑터 간 의존 금지)은 `ArchitectureTest`(ArchUnit)가 강제합니다.

## 기술 스택

| 구분 | 사용 기술 |
|---|---|
| 언어 | Java 25 |
| 프레임워크 | Spring Boot 4.1.1 (Web MVC, WebSocket, Kafka, Data JPA, Data Redis, Flyway, Validation, Actuator) |
| 빌드 | Gradle 9.7.1 (wrapper) |
| 메시징 | Apache Kafka |
| 최신 상태 | Redis (해시 + Pub/Sub) |
| 데이터베이스 | PostgreSQL + TimescaleDB (스키마는 Flyway로 관리, 테스트는 H2) |

## 시작하기

JDK 25와 Podman(또는 Docker) compose가 필요합니다.

```powershell
podman compose up -d                     # 이 레포의 compose.yaml: timescaledb(5432) + redis(6379)
podman compose ps                        # 둘 다 healthy 인지 확인
.\gradlew.bat bootRun                    # 서버 실행 (8080)
```

Git Bash, macOS, Linux에서는 `.\gradlew.bat` 대신 `./gradlew`를 사용합니다.

기본 접속 정보는 다음과 같고, 환경 변수로 바꿀 수 있습니다.

| 대상 | 기본값 | 환경 변수 |
|---|---|---|
| Kafka | `localhost:29092`, 토픽 `ocean.crane.indoor.event`, 그룹 `crane-integrated-server` | `KAFKA_BOOTSTRAP_SERVERS` |
| PostgreSQL | `jdbc:postgresql://localhost:5432/cis` (cis / cis) | `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` |
| Redis | `localhost:6379` | `REDIS_HOST`, `REDIS_PORT` |
| compose 포트 | 5432, 6379 | `CIS_DB_PORT`, `CIS_REDIS_PORT` |

그 밖의 동작 설정은 `application.yaml`의 `cis.*`에 있습니다(푸시·이력 주기, 큐 크기, stale 기준, 조회 상한, CORS 허용 오리진 등).

## API

REST (`/api`, 오류는 RFC 9457 ProblemDetail):

| 메서드·경로 | 파라미터 | 응답 |
|---|---|---|
| `GET /api/cranes` | – | 크레인 스냅샷 목록 |
| `GET /api/cranes/{craneId}` | – | 스냅샷 1건 (없으면 404) |
| `GET /api/cranes/{craneId}/history` | `from`, `to` (ISO-8601, 필수), `step` (기본 1s, 예: `5s`, `1m`) | baseline + step 간격 프레임(sample-and-hold) |
| `GET /api/cranes/{craneId}/changes` | `from`, `to`, `after` (커서), `limit` (기본 500, 최대 5000) | 상태가 변한 시점의 행 + `nextCursor` |

범위는 최대 1시간, 프레임 수는 최대 5000개이며 넘으면 400입니다.

스냅샷 형태:

```json
{"craneId":"C_867","stationId":"isl-ocean-crane","bay":"F5_1_Bay",
 "snapshotAt":"2026-10-19T05:23:16.993Z","lastReceivedAt":"2026-10-19T05:23:17.001Z","online":true,
 "tags":{"tl_status":{"value":false,"quality":0,"timestamp":"2026-10-19T05:23:16.993Z"}, "...":{}}}
```

WebSocket (STOMP, SockJS 없음):

- 엔드포인트 `ws://localhost:8080/ws`
- `/topic/cranes` — 250ms 틱마다 변한 크레인 묶음 `{"type":"snapshot","sentAt":...,"cranes":[스냅샷...]}`
- `/topic/cranes/{craneId}` — 크레인별 스냅샷 1건
- 구독 직후 초기 화면은 `GET /api/cranes`로 채웁니다.

허용 오리진은 `cis.web.allowed-origins`(기본 `http://localhost:*`)로 REST CORS와 STOMP 핸드셰이크에 같이 적용됩니다.

## 확인 방법

```
GET http://localhost:8080/actuator/health      # db, redis 인디케이터 포함
GET http://localhost:8080/actuator/metrics     # cis.ingest.*, cis.history.*, cis.ws.sessions, cis.cranes.*
```

- 서버 로그에 60초마다 요약 한 줄이 찍힙니다: `ingest received=… changed=… skipped=… | history rows=…/… queue=… | ws sessions=… | cranes online=…/…`
- Redis: `podman exec cis-redis-1 redis-cli HLEN cis:crane:state` (크레인 수), `SUBSCRIBE cis:crane:snapshots`
- DB: `psql -h localhost -U cis -d cis -c "select crane_id, count(*) from crane_history group by 1"`
- DB(TimescaleDB)를 잠시 내려도 수집·푸시는 계속되고, 이력은 큐에 쌓였다가(`cis.history.queue_size`) 복구 후 빠집니다.

## 테스트

```powershell
.\gradlew.bat test
```

테스트는 `test` 프로필로 돌며 H2(PostgreSQL 모드)를 씁니다. Kafka는 EmbeddedKafka로, Redis는 in-memory 포트 구현으로 대체하므로 외부 인프라 없이 실행됩니다. `CraneIngestionEndToEndTest`가 실제 형식의 메시지를 넣어 REST 응답, DB 행, STOMP 수신까지 확인합니다.
