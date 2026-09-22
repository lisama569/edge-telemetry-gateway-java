package com.github.lisama569.edgetelemetrygatewayjava.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.config.GatewayProperties;
import com.github.lisama569.edgetelemetrygatewayjava.dedupe.DedupeDecision;
import com.github.lisama569.edgetelemetrygatewayjava.dedupe.DedupeResult;
import com.github.lisama569.edgetelemetrygatewayjava.dedupe.Deduplicator;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.NormalizerRegistry;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.ProtocolNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.ProtocolNormalizer.ParseOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 采集入口，处置结论分为四类（outcome）：
 * <ul>
 *   <li>ACCEPTED：归一通过、去重接受、已落盘，entryId 非空；</li>
 *   <li>REJECTED：报文无法识别，rejectReason 为 MALFORMED/MISSING_FIELD/BAD_VALUE/UNKNOWN_PROTOCOL；</li>
 *   <li>DUPLICATE：同一测点相同 seq 的重复报文，不重复计数；</li>
 *   <li>STALE：迟到/乱序且 seq 更小的旧报文，不允许覆盖新值；</li>
 *   <li>BACKPRESSURE_REJECTED：缓存持续占满、等待 offerTimeoutMs 后显式拒绝（非静默丢弃）。</li>
 * </ul>
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final NormalizerRegistry registry;
    private final Deduplicator deduplicator;
    private final PendingStore store;
    private final GatewayProperties properties;
    private final GatewayMetrics metrics;

    public IngestService(NormalizerRegistry registry, Deduplicator deduplicator,
                         PendingStore store, GatewayProperties properties, GatewayMetrics metrics) {
        this.registry = registry;
        this.deduplicator = deduplicator;
        this.store = store;
        this.properties = properties;
        this.metrics = metrics;
    }

    public IngestResult ingest(String protocol, String raw) {
        metrics.received.incrementAndGet();
        log.info("[ingest] 收到报文 protocol={} raw={}", protocol, raw);

        ProtocolNormalizer normalizer = registry.find(protocol).orElse(null);
        if (normalizer == null) {
            metrics.rejected.incrementAndGet();
            log.info("[ingest] 判定拒绝 reason={} protocol={}", RejectReason.UNKNOWN_PROTOCOL, protocol);
            return new IngestResult("REJECTED", RejectReason.UNKNOWN_PROTOCOL, -1L,
                    "未知协议: " + protocol + "（支持: json/csv）");
        }

        ParseOutcome parsed = normalizer.parse(raw);
        if (!parsed.accepted()) {
            metrics.rejected.incrementAndGet();
            return new IngestResult("REJECTED", parsed.rejectReason(), -1L, parsed.detail());
        }
        Reading reading = parsed.reading();

        DedupeResult result = deduplicator.decide(reading);
        DedupeDecision decision = result.decision();
        switch (decision) {
            case DUPLICATE -> {
                metrics.duplicate.incrementAndGet();
                return new IngestResult("DUPLICATE", RejectReason.DUPLICATE, -1L,
                        "重复报文，测点 " + reading.sourceId() + "/" + reading.pointId()
                                + " 已接受 seq=" + reading.seq());
            }
            case STALE -> {
                metrics.stale.incrementAndGet();
                return new IngestResult("STALE", RejectReason.STALE, -1L,
                        "迟到旧报文 seq=" + reading.seq()
                                + "，当前已接受最大 seq="
                                + deduplicator.acceptedSequenceOf(reading.sourceId(), reading.pointId()));
            }
            case ACCEPT -> {
                // 预留完成，落盘成功后才真正提交；落盘失败需回滚预留
            }
        }

        long entryId = store.append(reading, properties.getOfferTimeoutMs());
        if (entryId < 0) {
            deduplicator.release(reading, result.previousSeq());
            metrics.backpressureRejected.incrementAndGet();
            log.warn("[ingest] 判定背压拒绝 reading={} 缓存 pending={}/{} 等待超时 {}ms",
                    reading, store.pendingCount(), store.capacity(), properties.getOfferTimeoutMs());
            return new IngestResult("BACKPRESSURE_REJECTED", RejectReason.BACKPRESSURE_REJECTED, -1L,
                    "本地缓存已满 " + store.pendingCount() + "/" + store.capacity()
                            + "，等待 " + properties.getOfferTimeoutMs() + "ms 后仍无空间");
        }
        metrics.accepted.incrementAndGet();
        metrics.persisted.incrementAndGet();
        log.info("[ingest] 判定 ACCEPTED entryId={} reading={}", entryId, reading);
        return new IngestResult("ACCEPTED", null, entryId, null);
    }
}
