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
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final Map<String, ProtocolNormalizer> normalizers;
    private final DedupGate dedupGate;
    private final TelemetryBuffer buffer;

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong late = new AtomicLong();
    private final Map<RejectReason, AtomicLong> rejected = new ConcurrentHashMap<>();

    public IngestService(List<ProtocolNormalizer> normalizers,
                         DedupGate dedupGate,
                         TelemetryBuffer buffer) {
        this.normalizers = normalizers.stream()
                .collect(Collectors.toUnmodifiableMap(ProtocolNormalizer::protocol, Function.identity()));
        this.dedupGate = dedupGate;
        this.buffer = buffer;
    }

    public IngestResult ingest(RawFrame frame) {
        ProtocolNormalizer normalizer = normalizers.get(frame.protocol());
        if (normalizer == null) {
            return reject(RejectReason.UNKNOWN_PROTOCOL, "no normalizer for protocol: " + frame.protocol());
        }
        final IngestResult checked;
        try {
            checked = dedupGate.check(normalizer.normalize(frame));
        } catch (NormalizationException e) {
            return reject(e.reason(), e.getMessage());
        }
        switch (checked) {
            case IngestResult.Accepted a -> {
                accepted.incrementAndGet();
                TelemetryBuffer.AppendOutcome outcome = buffer.append(a.point());
                if (outcome.status() == TelemetryBuffer.AppendStatus.APPENDED_EVICTED_OLDEST) {
                    log.warn("buffer full, evicted oldest: {}", outcome.evicted());
                }
            }
            case IngestResult.Duplicate d -> {
                duplicates.incrementAndGet();
                log.debug("duplicate dropped: {}", d.point());
            }
            case IngestResult.Late l -> {
                late.incrementAndGet();
                log.debug("late frame dropped: {} (lastSeq={})", l.point(), l.lastSeenSeq());
            }
            case IngestResult.Rejected ignored -> {
                // 不可达：dedupGate 不会产生 Rejected
            }
        }
        return checked;
    }

    private IngestResult reject(RejectReason reason, String detail) {
        rejected.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
        log.info("frame rejected: reason={} detail={}", reason, detail);
        return new IngestResult.Rejected(reason, detail);
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
