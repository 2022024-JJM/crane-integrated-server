package com.hanwha.cis.support;

import com.hanwha.cis.application.port.out.SnapshotPublisherPort;
import com.hanwha.cis.domain.model.CraneSnapshot;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Redis 채널을 대신한다. 발행 묶음을 기록하고, 있으면 구독자(delegate)를 직접 호출한다. */
public class RecordingSnapshotPublisher implements SnapshotPublisherPort {

    private final List<List<CraneSnapshot>> published = new CopyOnWriteArrayList<>();
    private final Consumer<List<CraneSnapshot>> delegate;

    public RecordingSnapshotPublisher() {
        this(snapshots -> { });
    }

    public RecordingSnapshotPublisher(Consumer<List<CraneSnapshot>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void publish(List<CraneSnapshot> snapshots) {
        published.add(List.copyOf(snapshots));
        delegate.accept(snapshots);
    }

    public List<List<CraneSnapshot>> published() {
        return published;
    }
}
