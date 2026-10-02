package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 去重与乱序闸门：按测点 (sourceId/pointId) 维护已确认的最新 seq 与其内容指纹。
 *
 * 判定规则（确定性、可复现）：
 * - seq > lastSeq            -> Accepted，调用方入队成功后才推进水位；
 * - seq == lastSeq 且内容一致 -> Duplicate（重发，丢弃不计数）；
 * - seq < lastSeq，或 seq == lastSeq 但内容冲突 -> Late（迟到/乱序，绝不覆盖已有数据）。
 *
 * 关键原子性：{@link #checkAndEnqueue} 把“判定 + 实际入队”放在同一测点的串行临界区里。
 * 只有入队成功才推进水位；若入队因共享扇出缓存写满而失败（背压），判定为
 * {@link RejectReason#BUFFER_FULL} 且<strong>不推进水位</strong>——同一报文退避后重试仍是合法新数据，
 * 不会被误判成重复而永久丢失。
 *
 * 线程安全：按测点 key 加独立锁，多线程写不同测点互不阻塞，写同一测点顺序确定。
 */
@Component
public class DedupGate {

    private static final Logger log = LoggerFactory.getLogger(DedupGate.class);

    private record Seen(long seq, double value, String sampledAt) {
        static Seen of(TelemetryPoint p) {
            return new Seen(p.seq(), p.value(), p.sampledAt().toString());
        }
    }

    /** 入队动作：返回 true 表示已成功进入下游缓存；false 表示背压（缓存满），本次不推进水位。 */
    @FunctionalInterface
    public interface Enqueue {
        boolean enqueue(TelemetryPoint point);
    }

    private final Map<String, Seen> watermarks = new ConcurrentHashMap<>();
    /** 每测点一把锁，保证“判定 + 入队 + 推进水位”对同一测点原子且顺序确定。 */
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public IngestResult checkAndEnqueue(TelemetryPoint point, Enqueue enqueue) {
        Object lock = locks.computeIfAbsent(point.key(), k -> new Object());
        synchronized (lock) {
            Seen prev = watermarks.get(point.key());
            Seen incoming = Seen.of(point);
            boolean advances = prev == null || point.seq() > prev.seq();
            if (advances) {
                boolean enqueued;
                try {
                    enqueued = enqueue.enqueue(point);
                } catch (RuntimeException e) {
                    // 入队异常（如落盘失败）：不推进水位，交由上层按可重试失败处理，避免丢数据。
                    log.warn("dedup enqueue raised for key={} seq={}, watermark not advanced: {}",
                            point.key(), point.seq(), e.getMessage());
                    throw e;
                }
                if (!enqueued) {
                    log.info("dedup backpressure key={} seq={}: buffer full, watermark not advanced (retryable)",
                            point.key(), point.seq());
                    return new IngestResult.Rejected(RejectReason.BUFFER_FULL,
                            "downstream shared buffer full for " + point.key()
                                    + " seq=" + point.seq() + "; back off and retry; no data evicted");
                }
                watermarks.put(point.key(), incoming);
                log.debug("dedup check key={} seq={} -> ACCEPTED", point.key(), point.seq());
                return new IngestResult.Accepted(point);
            }
            IngestResult result;
            if (point.seq() == prev.seq() && incoming.equals(prev)) {
                result = new IngestResult.Duplicate(point);
            } else {
                result = new IngestResult.Late(point, prev.seq());
            }
            log.debug("dedup check key={} seq={} -> {}", point.key(), point.seq(),
                    result.getClass().getSimpleName());
            return result;
        }
    }

    public void reset() {
        watermarks.clear();
    }
}
