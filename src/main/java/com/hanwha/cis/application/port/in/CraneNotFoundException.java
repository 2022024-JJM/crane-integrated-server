package com.hanwha.cis.application.port.in;

public class CraneNotFoundException extends RuntimeException {

    private final String craneId;

    public CraneNotFoundException(String craneId) {
        super("crane not found: " + craneId);
        this.craneId = craneId;
    }

    public String craneId() {
        return craneId;
    }
}
