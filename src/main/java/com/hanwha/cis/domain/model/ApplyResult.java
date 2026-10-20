package com.hanwha.cis.domain.model;

/** {@link CraneState#apply} 결과. */
public enum ApplyResult {
    /** 새 태그이거나 값이 바뀌었다. */
    CHANGED,
    /** 값은 같고 timestamp/quality만 갱신했다. */
    UNCHANGED,
    /** 저장된 timestamp보다 이른 값이라 무시했다. */
    LATE_IGNORED
}
