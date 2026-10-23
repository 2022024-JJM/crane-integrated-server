package com.hanwha.cis.adapter.in.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 처리에 threshold(cis.web.slow-request-threshold) 이상 걸린 REST 요청만 WARN 한 줄로 남긴다(메서드·경로·쿼리·상태·소요 시간).
 * "리플레이가 느리다"는 문의에 어느 크레인·구간이었는지 답하기 위함이다. 건수·응답 시간 분포는 http.server.requests 메트릭에 있고
 * 헬스체크가 대부분을 차지하므로 요청마다 남기는 access log는 두지 않는다. /api/* 에만 건다(WebConfig).
 */
public class SlowRequestLogFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(SlowRequestLogFilter.class);
    private static final int MAX_TARGET_LENGTH = 300;

    private final long thresholdNanos;
    private final LongSupplier nanoTime;

    public SlowRequestLogFilter(Duration threshold) {
        this(threshold, System::nanoTime);
    }

    SlowRequestLogFilter(Duration threshold, LongSupplier nanoTime) {
        this.thresholdNanos = threshold.toNanos();
        this.nanoTime = nanoTime;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long start = nanoTime.getAsLong();
        String outcome = null;
        try {
            chain.doFilter(request, response);
        } catch (IOException | ServletException | RuntimeException e) {
            outcome = "failed: " + e.getClass().getSimpleName();
            throw e;
        } finally {
            long elapsed = nanoTime.getAsLong() - start;
            if (elapsed >= thresholdNanos) {
                log.warn("Slow request: {} {} -> {} in {} ms (remote {})", request.getMethod(), target(request),
                        outcome != null ? outcome : String.valueOf(response.getStatus()), elapsed / 1_000_000,
                        request.getRemoteAddr());
            }
        }
    }

    private static String target(HttpServletRequest request) {
        String query = request.getQueryString();
        String target = query == null ? request.getRequestURI() : request.getRequestURI() + "?" + query;
        return target.length() <= MAX_TARGET_LENGTH ? target : target.substring(0, MAX_TARGET_LENGTH) + "...";
    }
}
