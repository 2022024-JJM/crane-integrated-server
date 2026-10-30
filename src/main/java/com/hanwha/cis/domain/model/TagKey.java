package com.hanwha.cis.domain.model;

/**
 * 태그 식별자: 구역(bay)·크레인·태그 이름. 내업 TagKey는 {bay}.{craneId}.{tagName} 3단 문자열이라 {@link #parseIndoor}로 읽는다.
 * 외업은 bay에 PLC 장비 이름, tagName에 원본 TagKey 전체를 넣어 생성자로 만든다.
 */
public record TagKey(String bay, String craneId, String tagName) {

    public TagKey {
        requireSegment(bay, "bay");
        requireSegment(craneId, "craneId");
        requireSegment(tagName, "tagName");
    }

    /** 내업 TagKey {bay}.{craneId}.{tagName}. 정확히 3단이어야 한다. */
    public static TagKey parseIndoor(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("tagKey is null");
        }
        String[] parts = raw.split("\\.", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("tagKey must have exactly 3 segments: " + raw);
        }
        return new TagKey(parts[0], parts[1], parts[2]);
    }

    public String raw() {
        return bay + "." + craneId + "." + tagName;
    }

    private static void requireSegment(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " segment is blank");
        }
    }
}
