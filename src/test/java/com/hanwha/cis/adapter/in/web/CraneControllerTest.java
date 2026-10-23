package com.hanwha.cis.adapter.in.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;

import com.hanwha.cis.application.port.in.CraneNotFoundException;
import com.hanwha.cis.application.port.in.InvalidQueryException;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.Change;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.Frame;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryChanges;
import com.hanwha.cis.application.port.in.QueryCraneHistoryUseCase.HistoryFrames;
import com.hanwha.cis.application.port.in.QueryCraneStateUseCase;
import com.hanwha.cis.domain.model.CraneSnapshot;
import com.hanwha.cis.domain.model.TagState;
import com.hanwha.cis.domain.model.TagValue;
import com.hanwha.cis.support.LogCapture;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import org.springframework.transaction.CannotCreateTransactionException;

@WebMvcTest({CraneController.class, CraneHistoryController.class})
class CraneControllerTest {

    private static final Instant T0 = Instant.parse("2026-10-19T05:23:16.993Z");

    @Autowired
    private MockMvcTester mvc;

    @MockitoBean
    private QueryCraneStateUseCase stateQuery;

    @MockitoBean
    private QueryCraneHistoryUseCase historyQuery;

    private static CraneSnapshot snapshot() {
        return new CraneSnapshot("C_867", "isl-ocean-crane", "F5_1_Bay", T0, T0, true, Map.of(
                "tl_status", new TagState(TagValue.of(false), 0, T0),
                "hoist_height_value", new TagState(TagValue.of(120), 0, T0),
                "ratio", new TagState(TagValue.of(0.5), 0, T0)));
    }

    @Test
    void listsCranesAsJson() {
        given(stateQuery.list()).willReturn(List.of(snapshot()));

        MvcTestResult result = mvc.get().uri("/api/cranes").exchange();

        assertThat(result).hasStatusOk().hasContentTypeCompatibleWith(MediaType.APPLICATION_JSON);
        assertThat(result).bodyJson().extractingPath("$[0].craneId").isEqualTo("C_867");
        assertThat(result).bodyJson().extractingPath("$[0].online").isEqualTo(true);
        assertThat(result).bodyJson().extractingPath("$[0].snapshotAt").isEqualTo("2026-10-19T05:23:16.993Z");
        assertThat(result).bodyJson().extractingPath("$[0].tags.tl_status.value").isEqualTo(false);
        assertThat(result).bodyJson().extractingPath("$[0].tags.hoist_height_value.value").isEqualTo(120);
        assertThat(result).bodyJson().extractingPath("$[0].tags.ratio.value").isEqualTo(0.5);
        assertThat(result).bodyJson().extractingPath("$[0].tags.ratio.quality").isEqualTo(0);
    }

    @Test
    void returnsSingleCrane() {
        given(stateQuery.get("C_867")).willReturn(snapshot());

        MvcTestResult result = mvc.get().uri("/api/cranes/C_867").exchange();

        assertThat(result).hasStatusOk();
        assertThat(result).bodyJson().extractingPath("$.bay").isEqualTo("F5_1_Bay");
    }

    @Test
    void unknownCraneIsProblemDetail404() {
        given(stateQuery.get("C_999")).willThrow(new CraneNotFoundException("C_999"));

        MvcTestResult result = mvc.get().uri("/api/cranes/C_999").exchange();

        assertThat(result).hasStatus(404).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(result).bodyJson().extractingPath("$.status").isEqualTo(404);
        assertThat(result).bodyJson().extractingPath("$.title").isEqualTo("Crane not found");
        assertThat(result).bodyJson().extractingPath("$.craneId").isEqualTo("C_999");
        assertThat(result).bodyJson().extractingPath("$.detail").asString().contains("C_999");
    }

    @Test
    void invalidQueryIsProblemDetail400() {
        given(historyQuery.frames(eq("C_867"), any(), any(), any()))
                .willThrow(new InvalidQueryException("range must not exceed PT1H"));

        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", "2026-10-19T05:00:00Z")
                .param("to", "2026-10-19T07:00:00Z")
                .exchange();

        assertThat(result).hasStatus(400).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(result).bodyJson().extractingPath("$.title").isEqualTo("Invalid query");
        assertThat(result).bodyJson().extractingPath("$.detail").isEqualTo("range must not exceed PT1H");
    }

    @Test
    void databaseFailureIsProblemDetail503AndLoggedOnlyAtStartAndRecovery() {
        Instant from = Instant.parse("2026-10-19T05:00:00Z");
        Instant to = Instant.parse("2026-10-19T05:10:00Z");
        given(historyQuery.changes("C_867", from, to, null, null))
                .willThrow(new CannotCreateTransactionException("Could not open JPA EntityManager for transaction"))
                .willThrow(new DataAccessResourceFailureException("Connection refused"))
                .willReturn(new HistoryChanges(List.of(), null));

        try (LogCapture logs = LogCapture.of(CraneHistoryController.class)) {
            for (int i = 0; i < 2; i++) {
                MvcTestResult result = mvc.get().uri("/api/cranes/C_867/changes")
                        .param("from", from.toString()).param("to", to.toString())
                        .exchange();

                assertThat(result).hasStatus(503).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
                assertThat(result).bodyJson().extractingPath("$.title").isEqualTo("History unavailable");
            }
            MvcTestResult recovered = mvc.get().uri("/api/cranes/C_867/changes")
                    .param("from", from.toString()).param("to", to.toString())
                    .exchange();

            assertThat(recovered).hasStatusOk();
            assertThat(logs.lines()).hasSize(2);
            assertThat(logs.lines().get(0)).isEqualTo("WARN History query from DB failed:"
                    + " CannotCreateTransactionException: Could not open JPA EntityManager for transaction");
            assertThat(logs.lines().get(1)).startsWith("INFO History query from DB recovered after")
                    .endsWith("(2 failures)");
        }
    }

    @Test
    void missingRequiredParameterIsProblemDetail400() {
        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", "2026-10-19T05:00:00Z")
                .exchange();

        assertThat(result).hasStatus(400).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(result).bodyJson().extractingPath("$.status").isEqualTo(400);
    }

    @Test
    void unparseableInstantIsProblemDetail400() {
        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", "yesterday").param("to", "now")
                .exchange();

        assertThat(result).hasStatus(400).hasContentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    void invalidStepIsProblemDetail400() {
        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", "2026-10-19T05:00:00Z")
                .param("to", "2026-10-19T05:10:00Z")
                .param("step", "soon")
                .exchange();

        assertThat(result).hasStatus(400);
        assertThat(result).bodyJson().extractingPath("$.detail").asString().contains("invalid step");
    }

    @Test
    void historyPassesParsedStepAndRendersFrames() {
        Instant from = Instant.parse("2026-10-19T05:00:00Z");
        Instant to = Instant.parse("2026-10-19T05:00:10Z");
        Map<String, TagValue> values = Map.of("hoist_height_value", TagValue.of(120));
        given(historyQuery.frames("C_867", from, to, Duration.ofSeconds(5))).willReturn(new HistoryFrames(
                "C_867", from, to, Duration.ofSeconds(5), List.of("hoist_height_value"),
                new Frame(from.minusSeconds(1), values),
                List.of(new Frame(from, values), new Frame(from.plusSeconds(5), values), new Frame(to, values)),
                false));

        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", from.toString()).param("to", to.toString()).param("step", "5s")
                .exchange();

        assertThat(result).hasStatusOk();
        assertThat(result).bodyJson().extractingPath("$.step").isEqualTo("PT5S");
        assertThat(result).bodyJson().extractingPath("$.tags[0]").isEqualTo("hoist_height_value");
        assertThat(result).bodyJson().extractingPath("$.baseline.values.hoist_height_value").isEqualTo(120);
        assertThat(result).bodyJson().extractingPath("$.frames.length()").isEqualTo(3);
        assertThat(result).bodyJson().extractingPath("$.frames[1].time").isEqualTo("2026-10-19T05:00:05Z");
        assertThat(result).bodyJson().extractingPath("$.truncated").isEqualTo(false);
    }

    @Test
    void historyDefaultsStepWhenOmitted() {
        Instant from = Instant.parse("2026-10-19T05:00:00Z");
        Instant to = Instant.parse("2026-10-19T05:00:10Z");
        given(historyQuery.frames(eq("C_867"), eq(from), eq(to), isNull())).willReturn(new HistoryFrames(
                "C_867", from, to, Duration.ofSeconds(1), List.of(), null, List.of(), false));

        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/history")
                .param("from", from.toString()).param("to", to.toString())
                .exchange();

        assertThat(result).hasStatusOk();
        assertThat(result).bodyJson().extractingPath("$.baseline").isNull();
    }

    @Test
    void changesPassCursorAndLimit() {
        Instant from = Instant.parse("2026-10-19T05:00:00Z");
        Instant to = Instant.parse("2026-10-19T05:10:00Z");
        Instant after = Instant.parse("2026-10-19T05:05:00Z");
        given(historyQuery.changes("C_867", from, to, after, 2)).willReturn(new HistoryChanges(
                List.of(new Change(after.plusSeconds(1), true, Map.of("tl_status", TagValue.of(true))),
                        new Change(after.plusSeconds(2), false, Map.of())),
                after.plusSeconds(2)));

        MvcTestResult result = mvc.get().uri("/api/cranes/C_867/changes")
                .param("from", from.toString()).param("to", to.toString())
                .param("after", after.toString()).param("limit", "2")
                .exchange();

        assertThat(result).hasStatusOk();
        assertThat(result).bodyJson().extractingPath("$.items.length()").isEqualTo(2);
        assertThat(result).bodyJson().extractingPath("$.items[0].values.tl_status").isEqualTo(true);
        assertThat(result).bodyJson().extractingPath("$.items[1].online").isEqualTo(false);
        assertThat(result).bodyJson().extractingPath("$.nextCursor").isEqualTo("2026-10-19T05:05:02Z");
    }
}
