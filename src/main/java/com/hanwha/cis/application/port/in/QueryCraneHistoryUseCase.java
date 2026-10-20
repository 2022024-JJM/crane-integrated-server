package com.hanwha.cis.application.port.in;

import com.hanwha.cis.domain.model.TagValue;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** 저장된 이력 조회(REST). */
public interface QueryCraneHistoryUseCase {

    /**
     * from~to를 step 간격으로 샘플링한 프레임 시계열. from 이전 마지막 행을 baseline으로 먼저 적용한다.
     *
     * @param step null이면 기본 step
     * @throws CraneNotFoundException 모르는 크레인
     * @throws InvalidQueryException  범위·step·포인트 수 상한 위반
     */
    HistoryFrames frames(String craneId, Instant from, Instant to, Duration step);

    /**
     * 상태가 변한 시점의 행(diff)을 시간순으로, 커서 페이징.
     *
     * @param after 커서(직전 페이지 마지막 time). null이면 from부터
     * @param limit null이면 기본값
     */
    HistoryChanges changes(String craneId, Instant from, Instant to, Instant after, Integer limit);

    record Frame(Instant time, Map<String, TagValue> values) {
    }

    /**
     * @param tags      baseline과 프레임에 등장하는 태그 이름(정렬)
     * @param baseline  from 이전 마지막 행. 없으면 null
     * @param truncated 행 상한에 걸려 프레임이 to까지 가지 못했으면 true
     */
    record HistoryFrames(
            String craneId,
            Instant from,
            Instant to,
            Duration step,
            List<String> tags,
            Frame baseline,
            List<Frame> frames,
            boolean truncated) {
    }

    record Change(Instant time, boolean online, Map<String, TagValue> values) {
    }

    /** @param nextCursor 다음 페이지가 있으면 마지막 항목의 time, 없으면 null */
    record HistoryChanges(List<Change> items, Instant nextCursor) {
    }
}
