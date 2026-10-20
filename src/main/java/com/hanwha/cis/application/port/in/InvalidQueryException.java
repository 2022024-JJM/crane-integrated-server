package com.hanwha.cis.application.port.in;

/** 조회 파라미터가 허용 범위를 벗어났다(HTTP 400). */
public class InvalidQueryException extends RuntimeException {

    public InvalidQueryException(String message) {
        super(message);
    }
}
