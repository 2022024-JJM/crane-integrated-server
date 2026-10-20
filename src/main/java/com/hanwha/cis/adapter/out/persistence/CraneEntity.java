package com.hanwha.cis.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/** crane 테이블. 쓰기는 {@link HistoryWriter}가 MERGE로 하고, 이 엔티티는 조회와 스키마 검증용이다. */
@Entity
@Table(name = "crane")
public class CraneEntity {

    @Id
    @Column(name = "crane_id", length = 64, nullable = false)
    private String craneId;

    @Column(name = "station_id", length = 64, nullable = false)
    private String stationId;

    @Column(name = "bay", length = 64, nullable = false)
    private String bay;

    @Column(name = "first_seen_at", nullable = false)
    private Instant firstSeenAt;

    @Column(name = "last_seen_at", nullable = false)
    private Instant lastSeenAt;

    protected CraneEntity() {
    }

    public String getCraneId() {
        return craneId;
    }

    public String getStationId() {
        return stationId;
    }

    public String getBay() {
        return bay;
    }

    public Instant getFirstSeenAt() {
        return firstSeenAt;
    }

    public Instant getLastSeenAt() {
        return lastSeenAt;
    }
}
