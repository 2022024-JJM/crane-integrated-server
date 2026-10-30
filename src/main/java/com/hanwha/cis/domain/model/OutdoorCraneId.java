package com.hanwha.cis.domain.model;

import java.util.Optional;

/**
 * 외업 PLC가 슬롯마다 주는 Crane_ID 숫자를 craneId로 바꾼다. 백의 자리(4자리면 천의 자리)가 타입이다.
 * 101 → GC_01, 207 → TTC_07, 348 → TC_48, 3104 → TC_104, 417 → OC_17, 452 → CC_52, 501 → LLC_01.
 * 규칙은 SCAPE 외업 크레인 데이터 정의 문서의 크레인 ID 규칙과 같다. 0 이하는 미배정이고, 규칙에 없는 숫자는 ID_{숫자}다.
 */
public final class OutdoorCraneId {

    static final String UNKNOWN_PREFIX = "ID_";

    private OutdoorCraneId() {
    }

    /** 미배정(0 이하)이면 빈 값. */
    public static Optional<String> of(long craneId) {
        if (craneId <= 0) {
            return Optional.empty();
        }
        String known = known(craneId);
        return Optional.of(known != null ? known : UNKNOWN_PREFIX + craneId);
    }

    /** 타입 규칙에 있는 숫자인지. 규칙 밖 숫자를 경고하는 데 쓴다. */
    public static boolean isKnown(long craneId) {
        return known(craneId) != null;
    }

    private static String known(long craneId) {
        if (craneId >= 100 && craneId <= 999) {
            int number = (int) (craneId % 100);
            String type = type((int) (craneId / 100), number >= 50);
            return type == null ? null : "%s_%02d".formatted(type, number);
        }
        if (craneId >= 3000 && craneId <= 4999) {
            int number = (int) (craneId % 1000);
            String type = type((int) (craneId / 1000), number >= 500);
            return "%s_%03d".formatted(type, number);
        }
        return null;
    }

    /** 4는 뒤 숫자가 450(4자리면 4500) 이상이면 크롤러(CC), 아니면 천장(OC)이다. */
    private static String type(int digit, boolean upperHalf) {
        return switch (digit) {
            case 1 -> "GC";
            case 2 -> "TTC";
            case 3 -> "TC";
            case 4 -> upperHalf ? "CC" : "OC";
            case 5 -> "LLC";
            default -> null;
        };
    }
}
