package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 采集入口：协议归一 -> 去重/乱序判定 -> 接受的数据写入本地缓存。
 * 拒绝时返回可区分的 RejectReason；同时维护可观测计数。
 *
 * 判定结论（均可经 DecisionLog 按 decisionId 单独查询）：
 * ACCEPTED / DUPLICATE / LATE / REJECTED(UNKNOWN_PROTOCOL|MALFORMED_PAYLOAD|INVALID_FIELD|BUFFER_FULL)。
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final Map<String, ProtocolNormalizer> normalizers;
    private final DedupGate dedupGate;
    private final TelemetryBuffer buffer;
    private final DecisionLog decisionLog;

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong late = new AtomicLong();
    private final Map<RejectReason, AtomicLong> rejected = new ConcurrentHashMap<>();

    public IngestService(List<ProtocolNormalizer> normalizers,
                         DedupGate dedupGate,
                         TelemetryBuffer buffer) {
        this(normalizers, dedupGate, buffer, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public IngestService(List<ProtocolNormalizer> normalizers,
                         DedupGate dedupGate,
                         TelemetryBuffer buffer,
                         DecisionLog decisionLog) {
        this.normalizers = normalizers.stream()
                .collect(Collectors.toUnmodifiableMap(ProtocolNormalizer::protocol, Function.identity()));
        this.dedupGate = dedupGate;
        this.buffer = buffer;
        this.decisionLog = decisionLog;
    }

    public IngestResult ingest(RawFrame frame) {
        ProtocolNormalizer normalizer = normalizers.get(frame.protocol());
        if (normalizer == null) {
            return reject(frame, RejectReason.UNKNOWN_PROTOCOL,
                    "no normalizer for protocol: " + frame.protocol());
        }
        final IngestResult checked;
        try {
            checked = dedupGate.check(normalizer.normalize(frame));
        } catch (NormalizationException e) {
            return reject(frame, e.reason(), e.getMessage());
        }
        switch (checked) {
            case IngestResult.Accepted a -> {
                TelemetryBuffer.AppendOutcome outcome = buffer.append(a.point());
                if (outcome.status() == TelemetryBuffer.AppendStatus.APPENDED_EVICTED_OLDEST) {
                    log.warn("buffer full, evicted oldest: {}", outcome.evicted());
                    accepted.incrementAndGet();
                    return record(frame, checked);
                }
                if (outcome.status() == TelemetryBuffer.AppendStatus.REJECTED_BUFFER_FULL) {
                    // 多下游模式：容量到上限且队首仍被慢下游占用，确定性拒绝，绝不静默丢弃
                    return reject(frame, RejectReason.BUFFER_FULL,
                            "buffer full (capacity), pending downstream(s) still own the head; "
                                    + "point key=" + a.point().key() + " seq=" + a.point().seq());
                }
                accepted.incrementAndGet();
                return record(frame, checked);
            }
            case IngestResult.Duplicate d -> {
                duplicates.incrementAndGet();
                log.debug("duplicate dropped: {}", d.point());
                return record(frame, checked);
            }
            case IngestResult.Late l -> {
                late.incrementAndGet();
                log.debug("late frame dropped: {} (lastSeq={})", l.point(), l.lastSeenSeq());
                return record(frame, checked);
            }
            case IngestResult.Rejected ignored -> {
                // 不可达：dedupGate 不会产生 Rejected
                return record(frame, checked);
            }
        }
    }

    private IngestResult reject(RawFrame frame, RejectReason reason, String detail) {
        rejected.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
        log.info("frame rejected: reason={} detail={}", reason, detail);
        IngestResult result = new IngestResult.Rejected(reason, detail);
        record(frame, result);
        return result;
    }

    private IngestResult record(RawFrame frame, IngestResult result) {
        if (decisionLog != null) {
            decisionLog.record(frame.protocol(), frame.payload(), result);
        }
        return result;
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public long duplicateCount() {
        return duplicates.get();
    }

    public long lateCount() {
        return late.get();
    }

    public long rejectedCount(RejectReason reason) {
        AtomicLong c = rejected.get(reason);
        return c == null ? 0 : c.get();
    }
}
