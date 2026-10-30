package com.hanwha.cis.domain.model;

/**
 * 크레인이 속한 범위. ISL 토픽 이름의 범위(ocean.crane.{indoor|outdoor}.event)와 같고, 화면별 REST·STOMP 경로를 나누는 기준이다.
 */
public enum CraneScope {
    INDOOR("indoor"),
    OUTDOOR("outdoor");

    private final String id;

    CraneScope(String id) {
        this.id = id;
    }

    /** JSON·메트릭 태그에 쓰는 이름. */
    public String id() {
        return id;
    }

    public static CraneScope fromId(String id) {
        for (CraneScope scope : values()) {
            if (scope.id.equals(id)) {
                return scope;
            }
        }
        throw new IllegalArgumentException("unknown crane scope: " + id);
    }
}
