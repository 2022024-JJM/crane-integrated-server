package com.hanwha.cis.domain.model;

/**
 * 태그 값. 변경 감지 전에 정규화한다: 숫자는 모두 double로(1 == 1.0), "true"/"false"/"1" 같은 문자열도 해석한다.
 */
public sealed interface TagValue permits TagValue.BoolValue, TagValue.NumberValue {

    /** JSON 등으로 내보낼 원시 값(Boolean 또는 Double). */
    Object raw();

    record BoolValue(boolean value) implements TagValue {
        @Override
        public Object raw() {
            return value;
        }
    }

    record NumberValue(double value) implements TagValue {
        public NumberValue {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException("number value must be finite: " + value);
            }
        }

        /** 정수로 표현 가능하면 true (직렬화 시 12.0 대신 12로 쓰기 위함). */
        public boolean isIntegral() {
            return value == Math.rint(value) && Math.abs(value) < 1e15;
        }

        @Override
        public Object raw() {
            return value;
        }
    }

    static TagValue of(Object raw) {
        return switch (raw) {
            case null -> throw new IllegalArgumentException("tag value is null");
            case TagValue v -> v;
            case Boolean b -> new BoolValue(b);
            case Number n -> new NumberValue(n.doubleValue());
            case String s -> parse(s);
            default -> throw new IllegalArgumentException(
                    "unsupported tag value type: " + raw.getClass().getSimpleName());
        };
    }

    private static TagValue parse(String s) {
        String trimmed = s.trim();
        if (trimmed.equalsIgnoreCase("true")) {
            return new BoolValue(true);
        }
        if (trimmed.equalsIgnoreCase("false")) {
            return new BoolValue(false);
        }
        try {
            return new NumberValue(Double.parseDouble(trimmed));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unparseable tag value: " + s, e);
        }
    }
}
