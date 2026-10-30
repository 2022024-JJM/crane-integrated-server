package com.hanwha.cis.application.service;

import com.hanwha.cis.application.port.in.IngestOutdoorTagUseCase;
import com.hanwha.cis.application.port.in.RawTagEvent;
import com.hanwha.cis.application.service.IngestStats.IgnoreReason;
import com.hanwha.cis.application.service.IngestStats.SkipReason;
import com.hanwha.cis.domain.model.ApplyResult;
import com.hanwha.cis.domain.model.CraneScope;
import com.hanwha.cis.domain.model.OutdoorCraneId;
import com.hanwha.cis.domain.model.TagKey;
import com.hanwha.cis.domain.model.TagReading;
import com.hanwha.cis.domain.model.TagValue;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 외업(outdoor) 메시지를 크레인별 상태로 푼다. 외업 PLC는 크레인 슬롯 30개(CR01~CR30)와 공통 블록을 PLC 장비 이름 하나
 * (봉투 craneId·레코드 키)로 보내므로, 슬롯의 Crane_ID 값으로 크레인을 정해 그 슬롯의 태그를 그 크레인 태그로 넣는다.
 * 태그 이름과 값은 원본 그대로 두고, bay에는 PLC 장비 이름을 넣는다. 슬롯 밖의 공통 블록은 받지 않고 센다.
 * <p>
 * 슬롯마다 원본 태그의 최근 값을 보관한다. Crane_ID가 바뀌면 새 크레인의 태그를 슬롯 보관값으로 통째로 바꾼다. 태그 이름에
 * 슬롯 번호가 들어 있어 이전 슬롯 이름의 태그가 남지 않게 하기 위함이다. 슬롯에서 빠진 크레인은 갱신이 멈춰 stale 기준 뒤 offline이 된다.
 * <p>
 * outdoor 리스너 스레드 하나가 부른다(레코드 키가 하나라 파티션도 하나다). 슬롯 상태는 이 클래스만 만지고 ingest를 동기화한다.
 */
public class OutdoorTagIngestionService implements IngestOutdoorTagUseCase {

    private static final Logger log = LoggerFactory.getLogger(OutdoorTagIngestionService.class);
    /** 슬롯 태그: 4_Crane.4_CR{슬롯 두 자리}.{필드}. 그 밖의 TagKey는 공통 블록이다. */
    private static final Pattern SLOT_TAG = Pattern.compile("4_Crane\\.4_CR(\\d{2})\\.(.+)");
    /** 슬롯의 크레인 ID 필드. 같은 주소를 가리키는 4_POS.00_Crane_ID_{n}은 일반 태그로만 다룬다. */
    private static final Pattern CRANE_ID_FIELD = Pattern.compile("00_Crane_ID_\\d+");
    private static final int MAX_PAYLOAD_SAMPLE = 200;
    private static final int MAX_SKIP_LOGS_PER_MINUTE = 10;

    private final CraneStateRegistry registry;
    private final IngestStats stats;
    private final Clock clock;
    private final SkipLogLimiter skipLogLimiter = new SkipLogLimiter(MAX_SKIP_LOGS_PER_MINUTE);
    private final Map<SlotKey, Slot> slots = new HashMap<>();
    /** craneId → 그 크레인이 들어 있는 슬롯. 같은 Crane_ID가 두 슬롯에 오면 먼저 잡은 슬롯만 반영한다. */
    private final Map<String, SlotKey> owners = new HashMap<>();
    private final Set<Long> warnedUnknownIds = new HashSet<>();

    public OutdoorTagIngestionService(CraneStateRegistry registry, IngestStats stats, Clock clock) {
        this.registry = registry;
        this.stats = stats;
        this.clock = clock;
    }

    @Override
    public synchronized void ingest(RawTagEvent event) {
        stats.onReceived();
        String device = event.craneId();
        if (device == null || device.isBlank()) {
            skip(SkipReason.PARSE, "craneId is missing", event);
            return;
        }
        if (event.recordKey() != null && !event.recordKey().equals(device)) {
            skip(SkipReason.MISMATCH, "record key " + event.recordKey() + " != envelope craneId " + device, event);
            return;
        }
        if (event.occurredAt() == null) {
            skip(SkipReason.PARSE, "occurredAt is missing", event);
            return;
        }
        if (event.tagKey() == null) {
            skip(SkipReason.PARSE, "tagKey is missing", event);
            return;
        }
        Matcher slotTag = SLOT_TAG.matcher(event.tagKey());
        if (!slotTag.matches()) {
            stats.onIgnored(IgnoreReason.COMMON);
            return;
        }
        TagValue value;
        try {
            value = TagValue.of(event.value());
        } catch (IllegalArgumentException e) {
            skip(SkipReason.PARSE, e.getMessage(), event);
            return;
        }
        if (value instanceof TagValue.NullValue) {
            stats.onNull();
        }
        int quality = event.quality() == null ? 0 : event.quality();
        Buffered reading = new Buffered(event.stationId(), event.tagKey(), value, quality,
                Instant.ofEpochMilli(event.occurredAt()));

        SlotKey key = new SlotKey(device, Integer.parseInt(slotTag.group(1)));
        Slot slot = slots.computeIfAbsent(key, k -> new Slot());
        slot.keep(reading);
        Instant now = clock.instant();
        if (CRANE_ID_FIELD.matcher(slotTag.group(2)).matches()) {
            String craneId = craneIdOf(value);
            if (!Objects.equals(craneId, slot.craneId)) {
                assign(key, slot, craneId, now);
                return;
            }
        }
        if (slot.craneId == null) {
            stats.onIgnored(IgnoreReason.UNASSIGNED);
            return;
        }
        count(registry.apply(reading.toTagReading(device, slot.craneId), now));
    }

    @Override
    public void skipUnparseable(String recordKey, String payloadSample) {
        stats.onReceived();
        stats.onSkipped(SkipReason.PARSE);
        if (skipLogLimiter.tryAcquire(clock.millis())) {
            log.warn("Skipping unparseable outdoor message: key={} payload={}", recordKey, truncate(payloadSample));
        }
    }

    /** 슬롯의 크레인을 바꾼다. 새 크레인은 태그를 슬롯 보관값으로 통째로 바꾼다. craneId가 null이면 미배정이다. */
    private void assign(SlotKey key, Slot slot, String craneId, Instant now) {
        if (slot.craneId != null) {
            owners.remove(slot.craneId, key);
            slot.craneId = null;
        }
        if (craneId == null) {
            slot.blocked = null;
            stats.onIgnored(IgnoreReason.UNASSIGNED);
            return;
        }
        SlotKey owner = owners.get(craneId);
        if (owner != null) {
            if (!craneId.equals(slot.blocked)) {
                slot.blocked = craneId;
                log.warn("Outdoor slot {} reports crane {} that slot {} already has; keeping {} and ignoring {}"
                        + " until its Crane_ID changes", key, craneId, owner, owner, key);
            }
            stats.onIgnored(IgnoreReason.UNASSIGNED);
            return;
        }
        if (slot.blocked != null) {
            log.info("Outdoor slot {} now has crane {}", key, craneId);
            slot.blocked = null;
        }
        owners.put(craneId, key);
        slot.craneId = craneId;
        registry.replaceTags(craneId, CraneScope.OUTDOOR, slot.readings(key.device(), craneId), now);
        stats.onChanged();
    }

    /** Crane_ID 값 → craneId. 정수가 아니거나 0 이하면 미배정(null). */
    private String craneIdOf(TagValue value) {
        if (!(value instanceof TagValue.NumberValue number) || !number.isIntegral()) {
            return null;
        }
        long id = (long) number.value();
        String craneId = OutdoorCraneId.of(id).orElse(null);
        if (craneId != null && !OutdoorCraneId.isKnown(id) && warnedUnknownIds.add(id)) {
            log.warn("Outdoor Crane_ID {} does not follow the crane type rule; using crane id {}", id, craneId);
        }
        return craneId;
    }

    private void count(ApplyResult result) {
        switch (result) {
            case CHANGED -> stats.onChanged();
            case UNCHANGED -> { }
            case LATE_IGNORED -> stats.onSkipped(SkipReason.LATE);
        }
    }

    private void skip(SkipReason reason, String why, RawTagEvent event) {
        stats.onSkipped(reason);
        if (skipLogLimiter.tryAcquire(clock.millis())) {
            log.warn("Skipping outdoor message ({}): {} | key={} device={} tagKey={} value={}",
                    reason.tag(), why, event.recordKey(), event.craneId(), event.tagKey(),
                    truncate(String.valueOf(event.value())));
        }
    }

    private static String truncate(String text) {
        if (text == null || text.length() <= MAX_PAYLOAD_SAMPLE) {
            return text;
        }
        return text.substring(0, MAX_PAYLOAD_SAMPLE) + "...";
    }

    /** PLC 장비 하나의 슬롯 하나. 로그에는 장비 이름과 슬롯을 함께 남긴다(예: 장비/CR05). */
    private record SlotKey(String device, int slot) {
        @Override
        public String toString() {
            return "%s/CR%02d".formatted(device, slot);
        }
    }

    /** 슬롯 하나: 지금 들어 있는 크레인과 원본 태그별 최근 값. */
    private static final class Slot {
        private String craneId;
        /** 다른 슬롯이 이미 가진 Crane_ID라 반영하지 못하는 크레인. 경고를 한 번만 남기려고 기억한다. */
        private String blocked;
        private final Map<String, Buffered> latest = new HashMap<>();

        void keep(Buffered reading) {
            latest.merge(reading.tagKey(), reading,
                    (kept, next) -> next.occurredAt().isBefore(kept.occurredAt()) ? kept : next);
        }

        List<TagReading> readings(String device, String craneId) {
            return latest.values().stream().map(r -> r.toTagReading(device, craneId)).toList();
        }
    }

    /** 슬롯에 보관하는 원본 태그 1건(어느 크레인 것인지 정해지기 전). */
    private record Buffered(String stationId, String tagKey, TagValue value, int quality, Instant occurredAt) {
        TagReading toTagReading(String device, String craneId) {
            return new TagReading(CraneScope.OUTDOOR, stationId, new TagKey(device, craneId, tagKey), value,
                    quality, occurredAt);
        }
    }
}
