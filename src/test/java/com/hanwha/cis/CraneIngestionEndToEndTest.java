package com.hanwha.cis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.hanwha.cis.adapter.in.websocket.WebSocketSessionMetrics;
import com.hanwha.cis.application.service.IngestStats;
import com.hanwha.cis.application.service.IngestStats.IgnoreReason;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import com.hanwha.cis.config.ApplicationConfig;
import com.hanwha.cis.support.InMemoryPortsConfig;
import com.hanwha.cis.support.RecordingSnapshotPublisher;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.client.RestClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import tools.jackson.databind.json.JsonMapper;

/**
 * 전체 컨텍스트 테스트: EmbeddedKafka에 실제 형식의 메시지(내업 3크레인 × 34태그, 외업 PLC 슬롯)를 넣고
 * REST 응답 → DB 행(H2) → STOMP 수신까지 확인한다. Redis는 in-memory 포트 구현으로 대체한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = {CraneIngestionEndToEndTest.TOPIC, CraneIngestionEndToEndTest.OUTDOOR_TOPIC})
@Import(InMemoryPortsConfig.class)
class CraneIngestionEndToEndTest {

    static final String TOPIC = "ocean.crane.indoor.event";
    static final String OUTDOOR_TOPIC = "ocean.crane.outdoor.event";
    /** 외업 PLC 장비 이름(레코드 키·봉투 craneId). bay로 그대로 나간다. */
    private static final String OUTDOOR_DEVICE = "OUTDOOR_PLC";

    private static final Map<String, String> CRANE_BAYS = Map.of(
            "C_867", "F5_1_Bay", "C_871", "F5_2_Bay", "C_890", "F5_2_Bay");
    private static final List<String> BOOL_TAGS = IntStream.rangeClosed(1, 26)
            .mapToObj(i -> "status_" + i).toList();
    private static final List<String> INT_TAGS = List.of("hoist_height_value", "trolley_pos_value",
            "gantry_pos_value", "load_value", "g1_gear_distance_value", "g2_gear_distance_value",
            "g3_gear_distance_value", "g4_gear_distance_value");

    @LocalServerPort
    private int port;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JsonMapper json;

    @Autowired
    @Qualifier(ApplicationConfig.INDOOR_INGEST_STATS)
    private IngestStats ingestStats;

    @Autowired
    @Qualifier(ApplicationConfig.OUTDOOR_INGEST_STATS)
    private IngestStats outdoorIngestStats;

    @Autowired
    private RecordingSnapshotPublisher publisher;

    @Autowired
    private WebSocketSessionMetrics wsMetrics;

    @Test
    void ingestsPushesStoresAndServes() throws Exception {
        RestClient rest = RestClient.create("http://localhost:" + port);

        // STOMP 구독
        BlockingQueue<Map<String, Object>> allFrames = new LinkedBlockingQueue<>();
        BlockingQueue<Map<String, Object>> craneFrames = new LinkedBlockingQueue<>();
        WebSocketStompClient stomp = stompClient();
        List<Throwable> clientErrors = new CopyOnWriteArrayList<>();
        StompSession session = connect(stomp, clientErrors);
        session.subscribe("/topic/indoor/cranes", queueing(allFrames));
        session.subscribe("/topic/indoor/cranes/C_867", queueing(craneFrames));
        await().atMost(Duration.ofSeconds(5)).until(() -> wsMetrics.sessions() == 1);
        Thread.sleep(300);

        // 1차: 3크레인 × 34태그 + 건너뛰어야 할 메시지 2건
        long t1 = System.currentTimeMillis();
        CRANE_BAYS.forEach((craneId, bay) -> sendAllTags(craneId, bay, t1, false, 120));
        kafka.send(TOPIC, "C_867", "{not json");
        kafka.send(TOPIC, "C_867", message("F5_1_Bay", "C_871", "status_1", true, t1));
        kafka.flush();

        // REST 현재 상태 (컨슈머 그룹 조인 전에는 404이므로 예외도 재시도한다)
        await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            Map<String, Object> crane = getJson(rest, "/api/indoor/cranes/C_867");
            assertThat(tags(crane)).hasSize(34);
            assertThat(crane.get("online")).isEqualTo(true);
            assertThat(crane.get("bay")).isEqualTo("F5_1_Bay");
            assertThat(crane.get("stationId")).isEqualTo("isl-ocean-crane");
            assertThat(crane.get("snapshotAt")).isEqualTo(Instant.ofEpochMilli(t1).toString());
            assertThat(tagValue(crane, "status_1")).isEqualTo(false);
            assertThat(tagValue(crane, "hoist_height_value")).isEqualTo(120);
        });
        List<Map<String, Object>> cranes = rest.get().uri("/api/indoor/cranes").retrieve()
                .body(new ParameterizedTypeReference<>() { });
        assertThat(cranes).extracting(c -> c.get("craneId")).containsExactly("C_867", "C_871", "C_890");
        assertThat(ingestStats.skipped(SkipReason.PARSE)).isEqualTo(1);
        assertThat(ingestStats.skipped(SkipReason.MISMATCH)).isEqualTo(1);
        assertThat(ingestStats.changed()).isEqualTo(3 * 34);

        // DB 행 (H2, 공통 V1 마이그레이션)
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(count("crane_history", "C_867")).isGreaterThanOrEqualTo(1);
            assertThat(count("crane_history_diff", "C_867")).isGreaterThanOrEqualTo(1);
            assertThat(jdbc.queryForObject("select bay from crane where crane_id = ?", String.class, "C_867"))
                    .isEqualTo("F5_1_Bay");
        });
        assertThat(jdbc.queryForObject("select online from crane_history_diff where crane_id = ? "
                + "order by time limit 1", Boolean.class, "C_867")).isTrue();

        // STOMP 수신 (1차 푸시는 구독 전에 지나갔을 수 있으므로 값을 바꿔 2차 푸시를 일으킨다)
        long t2 = t1 + 1_000;
        allFrames.clear();
        craneFrames.clear();
        sendAllTags("C_867", "F5_1_Bay", t2, true, 121);
        kafka.flush();

        // 푸시 틱(50ms)이 배치 중간에 끼면 일부 태그만 바뀐 프레임이 먼저 오므로, 마지막 태그가 반영된 프레임을 기다린다.
        Map<String, Object> bundle = await().atMost(Duration.ofSeconds(15))
                .until(() -> allFrames.poll(100, TimeUnit.MILLISECONDS), m -> m != null && hasCrane(m, "C_867")
                        && Integer.valueOf(121).equals(tagValue(craneIn(m, "C_867"), "hoist_height_value")));
        assertThat(bundle.get("type")).isEqualTo("snapshot");
        assertThat(bundle.get("sentAt")).isNotNull();
        Map<String, Object> pushed = craneIn(bundle, "C_867");
        assertThat(tagValue(pushed, "status_1")).isEqualTo(true);
        assertThat(pushed.get("snapshotAt")).isEqualTo(Instant.ofEpochMilli(t2).toString());

        Map<String, Object> single = craneFrames.poll(10, TimeUnit.SECONDS);
        assertThat(single).isNotNull();
        assertThat(single.get("craneId")).isEqualTo("C_867");
        assertThat(publisher.published()).isNotEmpty();
        assertThat(clientErrors).isEmpty();

        // 이력 REST
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(count("crane_history_diff", "C_867")).isGreaterThanOrEqualTo(2));
        String from = Instant.ofEpochMilli(t1 - 2_000).toString();
        String to = Instant.ofEpochMilli(t2 + 2_000).toString();
        // to가 아직 미래면 현재 시각으로 잘리므로, to가 지난 뒤에 조회해 프레임 수를 고정한다.
        await().atMost(Duration.ofSeconds(10)).until(() -> System.currentTimeMillis() > t2 + 2_000);
        Map<String, Object> history = getJson(rest,
                "/api/indoor/cranes/C_867/history?from=" + from + "&to=" + to + "&step=1s");
        assertThat(history.get("craneId")).isEqualTo("C_867");
        assertThat(history.get("step")).isEqualTo("PT1S");
        assertThat((List<?>) history.get("tags")).hasSize(34);
        assertThat((List<?>) history.get("frames")).hasSize(6); // from..to = 5초, step 1초 → 0..5초 6프레임
        assertThat(history.get("truncated")).isEqualTo(false);
        @SuppressWarnings("unchecked")
        Map<String, Object> lastFrame = (Map<String, Object>) ((List<?>) history.get("frames")).getLast();
        @SuppressWarnings("unchecked")
        Map<String, Object> lastValues = (Map<String, Object>) lastFrame.get("values");
        assertThat(lastValues.get("hoist_height_value")).isEqualTo(121);

        Map<String, Object> changes = getJson(rest, "/api/indoor/cranes/C_867/changes?from=" + from + "&to=" + to);
        List<?> items = (List<?>) changes.get("items");
        assertThat(items.size()).isGreaterThanOrEqualTo(2);
        assertThat(changes.get("nextCursor")).isNull();

        // 오류 응답
        assertThat(status(rest, "/api/indoor/cranes/C_999").value()).isEqualTo(404);
        assertThat(status(rest, "/api/cranes").value()).isEqualTo(404); // 범위 없는 옛 경로는 없다
        assertThat(status(rest, "/api/outdoor/cranes/C_867").value()).isEqualTo(404);
        assertThat(status(rest, "/api/indoor/cranes/C_867/history?from=" + to + "&to=" + from).value()).isEqualTo(400);
        Instant future = Instant.now().plusSeconds(60);
        assertThat(status(rest, "/api/indoor/cranes/C_867/history?from=" + future + "&to=" + future.plusSeconds(10))
                .value()).isEqualTo(400);

        // 컨테이너 헬스체크가 보는 liveness 프로브
        assertThat(getJson(rest, "/actuator/health/liveness").get("status")).isEqualTo("UP");

        session.disconnect();
        stomp.stop();
        await().atMost(Duration.ofSeconds(5)).until(() -> wsMetrics.sessions() == 0);
    }

    @Test
    void splitsOutdoorPlcSlotsIntoCranesOnTheOutdoorPaths() throws Exception {
        RestClient rest = RestClient.create("http://localhost:" + port);

        BlockingQueue<Map<String, Object>> allFrames = new LinkedBlockingQueue<>();
        BlockingQueue<Map<String, Object>> craneFrames = new LinkedBlockingQueue<>();
        BlockingQueue<Map<String, Object>> indoorFrames = new LinkedBlockingQueue<>();
        WebSocketStompClient stomp = stompClient();
        List<Throwable> clientErrors = new CopyOnWriteArrayList<>();
        StompSession session = connect(stomp, clientErrors);
        session.subscribe("/topic/outdoor/cranes", queueing(allFrames));
        session.subscribe("/topic/outdoor/cranes/GC_01", queueing(craneFrames));
        session.subscribe("/topic/indoor/cranes/GC_01", queueing(indoorFrames));
        await().atMost(Duration.ofSeconds(5)).until(() -> wsMetrics.sessions() == 1);
        Thread.sleep(300);

        // 슬롯 1 = Crane_ID 101(GC_01), 슬롯 20 = 미배정, 공통 블록 1건
        long t1 = System.currentTimeMillis();
        sendOutdoor("4_Crane.4_CR01.00_Crane_ID_1", 101, t1);
        sendOutdoor("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2000, t1);
        sendOutdoor("4_Crane.4_CR01.2_slow.52_TL_FW_slow_1", "\"NULL\"", t1);
        sendOutdoor("4_Crane.4_CR20.00_Crane_ID_20", 0, t1);
        sendOutdoor("4_Crane.4_CR20.4_POS.03_Center_POS_x20", 0, t1);
        sendOutdoor("1_Time.01_Year", 2026, t1);
        kafka.flush();

        await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
            Map<String, Object> crane = getJson(rest, "/api/outdoor/cranes/GC_01");
            assertThat(crane.get("scope")).isEqualTo("outdoor");
            assertThat(crane.get("bay")).isEqualTo(OUTDOOR_DEVICE);
            assertThat(tags(crane)).containsOnlyKeys("4_Crane.4_CR01.00_Crane_ID_1",
                    "4_Crane.4_CR01.4_POS.03_Center_POS_x1", "4_Crane.4_CR01.2_slow.52_TL_FW_slow_1");
            assertThat(tagValue(crane, "4_Crane.4_CR01.4_POS.03_Center_POS_x1")).isEqualTo(2000);
            assertThat(tag(crane, "4_Crane.4_CR01.2_slow.52_TL_FW_slow_1")).containsEntry("value", null);
        });
        List<Map<String, Object>> outdoorCranes = rest.get().uri("/api/outdoor/cranes").retrieve()
                .body(new ParameterizedTypeReference<>() { });
        assertThat(outdoorCranes).extracting(c -> c.get("craneId")).containsExactly("GC_01");
        List<Map<String, Object>> indoorCranes = rest.get().uri("/api/indoor/cranes").retrieve()
                .body(new ParameterizedTypeReference<>() { });
        assertThat(indoorCranes).extracting(c -> c.get("craneId")).doesNotContain("GC_01");
        assertThat(status(rest, "/api/indoor/cranes/GC_01").value()).isEqualTo(404);
        // 같은 키라 순서대로 처리된다. GC_01 뒤에 보낸 미배정 슬롯·공통 블록까지 처리되기를 기다린다.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(outdoorIngestStats.ignored(IgnoreReason.UNASSIGNED)).isEqualTo(2);
            assertThat(outdoorIngestStats.ignored(IgnoreReason.COMMON)).isEqualTo(1);
        });
        assertThat(outdoorIngestStats.nulls()).isEqualTo(1);
        assertThat(outdoorIngestStats.skippedTotal()).isZero();

        // STOMP: 값을 바꿔 outdoor 토픽으로 푸시가 가는지 본다
        long t2 = t1 + 1_000;
        allFrames.clear();
        craneFrames.clear();
        sendOutdoor("4_Crane.4_CR01.4_POS.03_Center_POS_x1", 2100, t2);
        kafka.flush();
        Map<String, Object> bundle = await().atMost(Duration.ofSeconds(15))
                .until(() -> allFrames.poll(100, TimeUnit.MILLISECONDS), m -> m != null && hasCrane(m, "GC_01")
                        && Integer.valueOf(2100).equals(
                                tagValue(craneIn(m, "GC_01"), "4_Crane.4_CR01.4_POS.03_Center_POS_x1")));
        assertThat(bundle.get("type")).isEqualTo("snapshot");
        Map<String, Object> single = craneFrames.poll(10, TimeUnit.SECONDS);
        assertThat(single).isNotNull().containsEntry("craneId", "GC_01");
        assertThat(indoorFrames).isEmpty();
        assertThat(clientErrors).isEmpty();

        // DB와 이력 REST(외업 경로)
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(count("crane_history_diff", "GC_01")).isGreaterThanOrEqualTo(2);
            assertThat(jdbc.queryForObject("select bay from crane where crane_id = ?", String.class, "GC_01"))
                    .isEqualTo(OUTDOOR_DEVICE);
        });
        String from = Instant.ofEpochMilli(t1 - 2_000).toString();
        String to = Instant.ofEpochMilli(t2 + 2_000).toString();
        // to가 아직 미래면 현재 시각으로 잘려 t2 행이 빠지므로, to가 지난 뒤에 조회한다.
        await().atMost(Duration.ofSeconds(10)).until(() -> System.currentTimeMillis() > t2 + 2_000);
        Map<String, Object> changes = getJson(rest, "/api/outdoor/cranes/GC_01/changes?from=" + from + "&to=" + to);
        assertThat((List<?>) changes.get("items")).hasSizeGreaterThanOrEqualTo(2);

        session.disconnect();
        stomp.stop();
        await().atMost(Duration.ofSeconds(5)).until(() -> wsMetrics.sessions() == 0);
    }

    /** 묶음 메시지(3크레인 × 34태그)는 Tomcat 클라이언트 기본 수신 버퍼(8KB)보다 크므로 버퍼를 키운다. */
    private WebSocketStompClient stompClient() {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxTextMessageBufferSize(1024 * 1024);
        WebSocketStompClient stomp = new WebSocketStompClient(new StandardWebSocketClient(container));
        stomp.setMessageConverter(new JacksonJsonMessageConverter(json));
        return stomp;
    }

    private StompSession connect(WebSocketStompClient stomp, List<Throwable> clientErrors) throws Exception {
        return stomp.connectAsync("ws://localhost:" + port + "/ws",
                new StompSessionHandlerAdapter() {
                    @Override
                    public void handleException(StompSession s, StompCommand command, StompHeaders headers,
                            byte[] payload, Throwable exception) {
                        clientErrors.add(exception);
                    }

                    @Override
                    public void handleTransportError(StompSession s, Throwable exception) {
                        clientErrors.add(exception);
                    }
                }).get(10, TimeUnit.SECONDS);
    }

    /** 외업 메시지: 레코드 키와 봉투 craneId가 PLC 장비 이름이고 TagKey는 원본 이름이다. value는 JSON 조각이다. */
    private void sendOutdoor(String tagKey, Object value, long occurredAt) {
        kafka.send(OUTDOOR_TOPIC, OUTDOOR_DEVICE, "{\"stationId\":\"isl-ocean-crane\",\"craneId\":\"%s\","
                .formatted(OUTDOOR_DEVICE)
                + "\"occurredAt\":%d,\"payload\":{\"TagKey\":\"%s\",\"Value\":%s,\"Timestamp\":\"%s\",\"Quality\":0}}"
                .formatted(occurredAt, tagKey, value, Instant.ofEpochMilli(occurredAt)));
    }

    private void sendAllTags(String craneId, String bay, long occurredAt, boolean boolValue, int intValue) {
        for (String tag : BOOL_TAGS) {
            kafka.send(TOPIC, craneId, message(bay, craneId, tag, boolValue, occurredAt));
        }
        for (String tag : INT_TAGS) {
            kafka.send(TOPIC, craneId, message(bay, craneId, tag, intValue, occurredAt));
        }
    }

    private static String message(String bay, String craneId, String tag, Object value, long occurredAt) {
        return "{\"stationId\":\"isl-ocean-crane\",\"craneId\":\"%s\",\"occurredAt\":%d,"
                .formatted(craneId, occurredAt)
                + "\"payload\":{\"TagKey\":\"%s.%s.%s\",\"Value\":%s,\"Timestamp\":\"%s\",\"Quality\":0}}"
                .formatted(bay, craneId, tag, value, Instant.ofEpochMilli(occurredAt));
    }

    private long count(String table, String craneId) {
        Long n = jdbc.queryForObject("select count(*) from " + table + " where crane_id = ?", Long.class, craneId);
        return n == null ? 0 : n;
    }

    private static Map<String, Object> getJson(RestClient rest, String uri) {
        return rest.get().uri(uri).retrieve().body(new ParameterizedTypeReference<>() { });
    }

    private static HttpStatusCode status(RestClient rest, String uri) {
        ResponseEntity<String> entity = rest.get().uri(uri).retrieve()
                .onStatus(HttpStatusCode::isError, (request, response) -> { })
                .toEntity(String.class);
        return entity.getStatusCode();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> tags(Map<String, Object> crane) {
        return (Map<String, Object>) crane.get("tags");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> tag(Map<String, Object> crane, String tag) {
        return (Map<String, Object>) tags(crane).get(tag);
    }

    @SuppressWarnings("unchecked")
    private static Object tagValue(Map<String, Object> crane, String tag) {
        return ((Map<String, Object>) tags(crane).get(tag)).get("value");
    }

    @SuppressWarnings("unchecked")
    private static boolean hasCrane(Map<String, Object> bundle, String craneId) {
        return ((List<Map<String, Object>>) bundle.get("cranes")).stream()
                .anyMatch(c -> craneId.equals(c.get("craneId")));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> craneIn(Map<String, Object> bundle, String craneId) {
        return ((List<Map<String, Object>>) bundle.get("cranes")).stream()
                .filter(c -> craneId.equals(c.get("craneId")))
                .findFirst().orElseThrow();
    }

    private static StompFrameHandler queueing(BlockingQueue<Map<String, Object>> queue) {
        return new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return Map.class;
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleFrame(StompHeaders headers, Object payload) {
                queue.add((Map<String, Object>) payload);
            }
        };
    }
}
