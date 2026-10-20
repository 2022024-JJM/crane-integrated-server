package com.hanwha.cis.domain.model;

/** 태그 식별자 {bay}.{craneId}.{tagName}. 정확히 3단이어야 한다. */
public record TagKey(String bay, String craneId, String tagName) {

    public TagKey {
        requireSegment(bay, "bay");
        requireSegment(craneId, "craneId");
        requireSegment(tagName, "tagName");
    }

    public static TagKey parse(String raw) {
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
