package com.hanwha.cis.application.support;

import java.time.Duration;

/** 로그용 사람이 읽는 시간 표기: 250ms, 45s, 3m 12s, 2h 5m. */
public final class Durations {

    private Durations() {
    }

    public static String format(Duration duration) {
        long millis = Math.max(0, duration.toMillis());
        if (millis < 1_000) {
            return millis + "ms";
        }
        long seconds = millis / 1_000;
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m " + seconds % 60 + "s";
        }
        long hours = minutes / 60;
        if (hours < 24) {
            return hours + "h " + minutes % 60 + "m";
        }
        return hours / 24 + "d " + hours % 24 + "h";
    }
}
