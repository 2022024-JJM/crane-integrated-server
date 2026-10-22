package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.application.port.in.RecordHistoryUseCase;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.config.SchedulingConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/** cis.history.interval(기본 1s)마다 이력 행을 쓰기 큐에 넣는다. */
@Component
public class HistoryRecordScheduler extends FixedDelayScheduler {

    private final RecordHistoryUseCase record;

    public HistoryRecordScheduler(RecordHistoryUseCase record,
            @Qualifier(SchedulingConfig.HISTORY_SCHEDULER) ThreadPoolTaskScheduler scheduler,
            CisProperties properties) {
        super(scheduler, properties.history().interval(), "History recording");
        this.record = record;
    }

    @Override
    protected void tick() {
        record.recordTick();
    }
}
