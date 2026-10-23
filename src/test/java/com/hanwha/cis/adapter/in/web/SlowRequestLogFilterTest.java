package com.hanwha.cis.adapter.in.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import com.hanwha.cis.support.LogCapture;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class SlowRequestLogFilterTest {

    private final AtomicLong nanos = new AtomicLong();
    private final SlowRequestLogFilter filter = new SlowRequestLogFilter(Duration.ofSeconds(1), nanos::get);
    private final LogCapture logs = LogCapture.of(SlowRequestLogFilter.class);

    @AfterEach
    void tearDown() {
        logs.close();
    }

    @Test
    void logsOnlyRequestsAtOrAboveTheThreshold() throws Exception {
        filter.doFilter(historyRequest(), new MockHttpServletResponse(), respondAfter(Duration.ofMillis(999), 200));
        assertThat(logs.lines()).isEmpty();

        filter.doFilter(historyRequest(), new MockHttpServletResponse(), respondAfter(Duration.ofMillis(2345), 503));

        assertThat(logs.lines()).containsExactly("WARN Slow request: GET /api/cranes/C_867/history"
                + "?from=2026-10-19T05:00:00Z&to=2026-10-19T06:00:00Z&step=1s -> 503 in 2345 ms (remote 10.0.0.5)");
    }

    @Test
    void slowFailureIsLoggedWithTheExceptionAndRethrown() {
        FilterChain failing = (request, response) -> {
            nanos.addAndGet(Duration.ofSeconds(3).toNanos());
            throw new IllegalStateException("boom");
        };

        assertThatIllegalStateException()
                .isThrownBy(() -> filter.doFilter(historyRequest(), new MockHttpServletResponse(), failing));

        assertThat(logs.lines()).singleElement().asString()
                .endsWith("-> failed: IllegalStateException in 3000 ms (remote 10.0.0.5)");
    }

    private static MockHttpServletRequest historyRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/cranes/C_867/history");
        request.setQueryString("from=2026-10-19T05:00:00Z&to=2026-10-19T06:00:00Z&step=1s");
        request.setRemoteAddr("10.0.0.5");
        return request;
    }

    private FilterChain respondAfter(Duration elapsed, int status) {
        return (request, response) -> {
            nanos.addAndGet(elapsed.toNanos());
            ((HttpServletResponse) response).setStatus(status);
        };
    }
}
