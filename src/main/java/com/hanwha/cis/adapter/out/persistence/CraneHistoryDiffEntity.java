package com.hanwha.cis.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/** crane_history_diff: 값 또는 online이 바뀐 시점의 전체 상태 행. */
@Entity
@Table(name = "crane_history_diff")
public class CraneHistoryDiffEntity {

    @EmbeddedId
    private CraneHistoryId id;

    @Column(name = "bay", length = 64, nullable = false)
    private String bay;

    @Column(name = "online", nullable = false)
    private boolean online;

    @Column(name = "state", nullable = false)
    private String state;

    protected CraneHistoryDiffEntity() {
    }

    public CraneHistoryId getId() {
        return id;
    }

    public String getBay() {
        return bay;
    }

    public boolean isOnline() {
        return online;
    }

    public String getState() {
        return state;
    }
}
