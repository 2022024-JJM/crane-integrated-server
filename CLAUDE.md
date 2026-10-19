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
