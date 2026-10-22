package com.hanwha.cis.config;

import com.hanwha.cis.application.support.Durations;
import com.hanwha.cis.application.support.FailureLog;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * 리스너 예외로 건너뛴 레코드를 기록한다. 같은 원인(가장 안쪽 예외 클래스)은 1분에 한 건만 스택트레이스와 함께 남기고,
 * 그 사이 건수는 다음 줄에 붙인다. 버그가 모든 메시지에서 터져도 로그가 메시지 수만큼 쏟아지지 않게 하기 위함이다.
 */
final class SkippedRecordLogger implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(SkippedRecordLogger.class);
    private static final long WINDOW_MILLIS = 60_000;

    private final RepeatWindows windows = new RepeatWindows(WINDOW_MILLIS, 100);

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception e) {
        long now = System.currentTimeMillis();
        RepeatWindows.Pass pass = windows.tryPass(rootCause(e).getClass().getName(), now);
        if (pass == null) {
            return;
        }
        String where = record.topic() + "-" + record.partition() + "@" + record.offset() + " key=" + record.key();
        if (pass.suppressed() == 0) {
            log.error("Kafka listener failed, record skipped: {}: {}", where, FailureLog.describe(e), e);
        } else {
            log.error("Kafka listener failed, record skipped: {}: {} (+{} more skipped for the same cause in the"
                    + " last {})", where, FailureLog.describe(e), pass.suppressed(),
                    Durations.format(Duration.ofMillis(now - pass.previousPassAt())), e);
        }
    }

    private static Throwable rootCause(Throwable e) {
        Throwable current = e;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
