package com.hanwha.cis.adapter.in.web;

/** 이력 DB를 읽지 못함. 로그는 {@link CraneHistoryController}가 남기고, {@link ApiExceptionHandler}가 503으로 바꾼다. */
class HistoryUnavailableException extends RuntimeException {

    HistoryUnavailableException(Throwable cause) {
        super(cause);
    }
}
