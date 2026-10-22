package com.hanwha.cis.adapter.in.scheduler;

import com.hanwha.cis.application.port.in.PushSnapshotsUseCase;
import com.hanwha.cis.config.CisProperties;
import com.hanwha.cis.config.SchedulingConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/** cis.push.interval(기본 250ms)마다 변한 크레인을 저장·발행한다. */
@Component
public class SnapshotPushScheduler extends FixedDelayScheduler {

    private final PushSnapshotsUseCase push;

    public SnapshotPushScheduler(PushSnapshotsUseCase push,
            @Qualifier(SchedulingConfig.PUSH_SCHEDULER) ThreadPoolTaskScheduler scheduler,
            CisProperties properties) {
        super(scheduler, properties.push().interval(), "Snapshot push to Redis");
        this.push = push;
    }

    @Override
    protected void tick() {
        push.pushDirty();
    }
}
