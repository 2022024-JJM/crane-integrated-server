package com.hanwha.cis.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** crane_history / crane_history_diff 의 복합 키 (crane_id, time). */
@Embeddable
public class CraneHistoryId implements Serializable {

    @Column(name = "crane_id", length = 64, nullable = false)
    private String craneId;

    @Column(name = "time", nullable = false)
    private Instant time;

    protected CraneHistoryId() {
    }

    public CraneHistoryId(String craneId, Instant time) {
        this.craneId = craneId;
        this.time = time;
    }

    public String getCraneId() {
        return craneId;
    }

    public Instant getTime() {
        return time;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CraneHistoryId other
                && Objects.equals(craneId, other.craneId)
                && Objects.equals(time, other.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(craneId, time);
    }
}
