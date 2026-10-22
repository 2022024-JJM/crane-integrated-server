package com.hanwha.cis.config;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 키별 고정 시간 창. 창마다 첫 건만 통과시키고 나머지는 센다. 다음 통과 때 그 사이 생략한 건수를 돌려준다.
 * 키가 maxKeys를 넘으면 전부 비운다(형식 문자열 종류가 끝없이 늘어나는 경우 대비).
 */
final class RepeatWindows {

    /** 통과한 호출. suppressed는 직전 통과 이후 생략한 건수, previousPassAt은 직전 통과 시각(첫 통과면 -1). */
    record Pass(long suppressed, long previousPassAt) {
    }

    private final long windowMillis;
    private final int maxKeys;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

    RepeatWindows(long windowMillis, int maxKeys) {
        this.windowMillis = windowMillis;
        this.maxKeys = maxKeys;
    }

    /** 통과면 {@link Pass}, 생략이면 null. */
    Pass tryPass(String key, long nowMillis) {
        if (windows.size() > maxKeys) {
            windows.clear();
        }
        Window window = windows.computeIfAbsent(key, k -> new Window());
        synchronized (window) {
            if (window.passedAt >= 0 && nowMillis - window.passedAt < windowMillis) {
                window.suppressed++;
                return null;
            }
            Pass pass = new Pass(window.suppressed, window.passedAt);
            window.passedAt = nowMillis;
            window.suppressed = 0;
            return pass;
        }
    }

    private static final class Window {
        long passedAt = -1;
        long suppressed;
    }
}
