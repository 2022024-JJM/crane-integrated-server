# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 시스템 구성

이 서버는 크레인 데이터 파이프라인의 맨 끝단(소비·가공·저장·전달)만 담당한다. Node-RED나 MQTT와 직접 통신하지 않는다.

```
① 데이터 소스     ② IntersysLink v4                       ③ Kafka  ④ 이 서버                                             ⑤ 브라우저
Node-RED ─MQTT──▶ MQTT Agent ▶ Engine ▶ Kafka Provider ─▶ Topic ─▶ Kafka Consumer ▶ Service ▶ WebSocket(STOMP) / REST ─▶ 웹 모니터링 화면
                                                                                       │
                                                                                       ▼
                                                                               ⑥ PostgreSQL
```

- ①~③은 외부 시스템이다. 크레인 값은 Node-RED 크레인 시뮬레이터가 만들어 MQTT로 발행하고, 수집과 Kafka produce는 IntersysLink v4의 MQTT Agent가 하며, 이 서버는 토픽을 subscribe만 한다.
- 서버 내부 흐름은 `Kafka Consumer → Service(데이터 가공/처리) → WebSocket(STOMP) 푸시 + REST 조회`다.
- Service는 Kafka로 받은 데이터를 실시간 푸시와 별개로 ⑥ PostgreSQL(TimescaleDB 확장)에 저장한다. 리플레이 등 저장 데이터가 필요한 기능은 이 DB를 읽는다.

## 명령어

Windows 환경이다. PowerShell에서는 `.\gradlew.bat`, Git Bash에서는 `./gradlew`를 쓴다.

```powershell
.\gradlew.bat build                      # 컴파일 + 테스트 + jar
.\gradlew.bat test                       # 전체 테스트
.\gradlew.bat bootRun                    # 로컬 실행
```

```powershell
podman compose up -d                          # 개발용 인프라: DB + Redis (bootRun과 함께 씀)
podman compose --profile app up -d --build    # 서버까지 컨테이너로 (Containerfile 빌드)
```

린트/포맷터는 설정되어 있지 않다.

## 스택에서 주의할 점

Spring Boot 4.1.1, Java 25(toolchain), Gradle 9.7.1(wrapper) 조합이다.

- **스타터 이름**: Boot 4의 모듈화된 이름을 쓴다(`spring-boot-starter-webmvc`, `spring-boot-starter-kafka`).
- **테스트 스타터**: `spring-boot-starter-test` 하나가 아니라 스타터마다 짝이 되는 `-test` 스타터를 둔다. 스타터를 추가·제거할 때 `testImplementation`의 짝도 같이 맞춘다.
- **Jackson 3**: 클래스패스에 있는 것은 `tools.jackson.*`(3.x)이고 Jackson 2 databind는 없다. 어노테이션만 `com.fasterxml.jackson.annotation`을 그대로 쓴다.
- **Kafka JSON 역직렬화**: `JacksonJsonDeserializer` / `JacksonJsonSerializer`를 쓴다. 같은 패키지의 `JsonDeserializer` / `JsonSerializer`는 Jackson 2 기반이라 이 프로젝트에서는 쓸 수 없다.
- **DB 접근/스키마**: Spring Data JPA로 접근하고 스키마는 Flyway가 관리한다. Hibernate `ddl-auto`는 `validate`라 엔티티를 추가하면 마이그레이션도 같이 써야 한다.
- **마이그레이션 위치**: `db/migration/common`(모든 DB에서 도는 SQL)과 `db/migration/{vendor}`(해당 DB 전용 SQL)로 나뉜다. TimescaleDB 전용 구문(`create_hypertable` 등)은 `db/migration/postgresql`에 둔다. 두 위치를 합쳐 버전 번호가 겹치면 안 된다.
- **테스트 DB**: Gradle `test` 태스크가 `test` 프로필을 켜고, 이 프로필은 H2(PostgreSQL 모드, in-memory)를 쓴다. 따라서 `db/migration/postgresql`의 SQL은 테스트에서 실행되지 않는다. 테스트 전용 vendor 폴더는 `src/test/resources/db/migration/h2`다.
- **테스트 도구**: `spring-kafka-test`(EmbeddedKafka)와 Awaitility를 쓴다(테스트 스타터에 포함). DB는 H2를 쓰고 Testcontainers는 없다.
- **Actuator**: 기본 설정이라 HTTP로는 `/actuator/health`만 노출된다. 다른 엔드포인트는 `management.endpoints.web.exposure.include`에 추가해야 한다.

## 규칙

- 루트 패키지는 `com.hanwha.cis`다.
- 설정 파일은 `application.yaml`(YAML)이다.

## 코드 구조 규칙 (헥사고날)

- 패키지는 `domain / application / adapter / config`로 나뉘고 의존 방향은 `domain ← application ← adapter, config`다. 어댑터(`adapter.in.*`, `adapter.out.*`)끼리는 서로 의존하지 않는다. `ArchitectureTest`(ArchUnit)가 이를 검사하므로 위반하면 테스트가 실패한다.
- `domain`과 `application`에는 Spring·JPA·Jackson 등 프레임워크 코드를 두지 않는다. 애플리케이션 서비스는 스테레오타입 어노테이션 없이 `config/ApplicationConfig`에서 `@Bean`으로 조립하고, 설정값(`CisProperties`)은 거기서만 풀어 생성자로 넘긴다.
- 도메인 타입의 JSON 표현은 `config/TagValueJacksonComponent`(`@JacksonComponent`)로 등록한다. REST·Redis·이력 JSON·STOMP가 모두 Boot의 `JsonMapper` 빈 하나를 쓴다.
- 어댑터가 공유해야 하는 이름(Redis 키·채널, Kafka 토픽)은 어댑터 상수가 아니라 `cis.*` 설정(`CisProperties`)에 둔다.

## 스레드 규칙

- Kafka 리스너 스레드(파티션당 1개)는 메모리 상태(`CraneStateRegistry`)만 갱신한다. DB·Redis를 직접 호출하지 않는다.
- 푸시(250ms)·이력(1s)·요약 로그(60s)·수신 감시(5s)·푸시 감시(5s)는 각각 `SchedulingConfig`의 전용 single-thread 스케줄러에서 돈다. 새 주기 작업은 `FixedDelayScheduler`를 상속해 만들고 전용 스케줄러 빈을 추가한다.
- DB 쓰기는 `HistoryWriter` 스레드만 한다. 다른 스레드는 `enqueue`로 큐에 넣는다(큐가 차면 버리고 카운트).
- STOMP 전송(`SimpMessagingTemplate`)은 Redis 구독 executor(`redis-sub-`)에서 한다.

## 저장소 규칙

- Redis: HASH `cis:crane:state`(field=craneId, value=CraneSnapshot JSON), 채널 `cis:crane:snapshots`(변한 스냅샷 목록 JSON). 키 이름은 `cis.redis.*`.
- Redis는 없어도 서버가 떠야 하는 의존성이다. 없는 동안 수집·이력·REST(메모리 상태)는 계속되고 실시간 푸시만 멈춘다. 그래서 구독 컨테이너는 자동 시작을 끄고 `RedisSubscriptionStarter`가 성공할 때까지 재시도하며, Lettuce는 끊긴 동안 명령을 바로 거부한다(`REJECT_COMMANDS`, 타임아웃 2s). DB는 Flyway 검증이 기동 시 필요하므로 필수 의존성이다.
- `HistoryWriter`의 SQL은 PostgreSQL과 H2(PostgreSQL 모드) 양쪽에서 도는 문법만 쓴다: `INSERT ... ON CONFLICT DO NOTHING`(충돌 대상 컬럼 명시 없이), `MERGE INTO ... USING (VALUES ...)`. `ON CONFLICT (col) DO UPDATE`는 H2가 지원하지 않는다.
- 이력 행의 `time`은 틱 시각이 아니라 스냅샷의 `snapshotAt`이다. 수신 없이 `online`만 바뀐 diff 행은 감지 시각을 쓴다.

## 배포 규칙

- 배포는 컨테이너(podman)다. 이미지는 `Containerfile`(멀티 스테이지, bootJar), 실행은 compose `app` 서비스다. `app`은 프로필로 묶여 기본 `up`에는 뜨지 않는다(bootRun과 포트·Kafka 컨슈머 그룹이 겹치지 않게).
- 재시작 정책은 `unless-stopped`다. DB가 없으면 기동에 실패해 종료하고 재시작으로 다시 뜬다.
- 헬스 체크는 `/actuator/health/liveness`만 본다. liveness에 외부 의존성(Redis·DB·Kafka)을 넣지 않는다.
- 서버 컨테이너는 Kafka가 광고하는 주소로 붙어야 한다. ISL Kafka는 컨테이너 쪽으로 `kafka:9092`(네트워크 `isl-infra`)를 광고하므로 `app`은 그 외부 네트워크에 붙는다(`CIS_KAFKA_BOOTSTRAP_SERVERS`, `CIS_KAFKA_NETWORK`).

## 로그 규칙

- 로그는 콘솔(`podman logs`)과 `${LOG_PATH}`의 `cis.log`(INFO 이상)·`cis-error.log`(WARN 이상)로 나가며, 파일을 남기려면 `LOG_PATH`를 볼륨에 연결한다. 출력 구성은 `logback-spring.xml`, 형식·경로·롤링 값은 `application.yaml`의 `logging.*`다. 시각은 KST로 찍는다.
- 로그 메시지는 영어로 쓴다(한글은 콘솔 인코딩에 따라 깨진다).
- ERROR는 버그(처리하지 못한 예외)와 데이터 유실(버린 이력 쓰기, `DroppedWritesLog`)에만 쓴다. 외부 시스템의 일시 장애는 WARN이다.
- 같은 대상이 반복해서 실패하는 곳(외부 시스템 호출, 주기 작업)은 매번 찍지 않고 `application.support.FailureLog`로 시작·지속(1분마다)·복구만 남긴다. `FixedDelayScheduler`는 틱 실패를 이미 이렇게 남긴다.
- 외부 라이브러리의 반복 WARN·ERROR는 `RepeatedLogFilter`가 1분에 한 번으로 줄인다. 대상은 `logback-spring.xml`의 `loggerPrefix`이고, 시끄러운 INFO는 `logging.level`로 끈다(`org.apache.kafka`는 WARN).
- Kafka 수신 상태(브로커 연결·파티션 할당·메시지 유무)는 `IngestWatchScheduler`가 판정해 남긴다. 리스너 id `ingest`는 스레드 이름과 client.id에 쓰이고, `idIsGroup = false`라 그룹은 `spring.kafka.consumer.group-id`를 따른다.
- 실시간 푸시 경로(Redis 발행 → 구독 → STOMP)는 `PushWatchScheduler`가 `cis.push.published`와 `cis.push.forwarded`를 비교해 판정한다(`!test` 프로필에서만 뜬다).
- 테스트에서 로그를 검증할 때는 `support.LogCapture`를 쓴다.

## 테스트 프로필 규칙

- Redis 어댑터와 `RedisConfig`는 `@Profile("!test")`이고, `application-test.yaml`이 Redis 자동 구성을 제외한다. `@SpringBootTest`는 `@Import(InMemoryPortsConfig.class)`로 `CraneStateStorePort`·`SnapshotPublisherPort`의 in-memory 구현을 넣어야 뜬다.
- `@SpringBootTest`에는 `@EmbeddedKafka`가 필수다. `spring.kafka.bootstrap-servers`가 `${spring.embedded.kafka.brokers}`라서 없으면 컨텍스트가 뜨지 않는다.
- 테스트 프로필은 `auto-offset-reset=earliest`, 푸시 50ms, 이력 200ms다.
