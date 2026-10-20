package com.hanwha.cis.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/** crane_history: 크레인당 1초 주기 전체 상태 행. state는 CraneSnapshot JSON. */
@Entity
@Table(name = "crane_history")
public class CraneHistoryEntity {

    @EmbeddedId
    private CraneHistoryId id;

    @Column(name = "bay", length = 64, nullable = false)
    private String bay;

    @Column(name = "state", nullable = false)
    private String state;

    protected CraneHistoryEntity() {
    }

    public CraneHistoryId getId() {
        return id;
    }

    public String getBay() {
        return bay;
    }

    public String getState() {
        return state;
    }
}
