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
 * - seq > lastSeq            -> Accepted，并推进水位
 * - seq == lastSeq 且内容一致 -> Duplicate（重发，丢弃不计数）
 * - seq < lastSeq，或 seq == lastSeq 但内容冲突 -> Late（迟到/乱序，绝不覆盖已有数据）
 *
 * 线程安全：采集侧多线程写入时按 key 原子判定。
 */
@Component
public class DedupGate {

    private static final Logger log = LoggerFactory.getLogger(DedupGate.class);

    private record Seen(long seq, double value, String sampledAt) {
        static Seen of(TelemetryPoint p) {
            return new Seen(p.seq(), p.value(), p.sampledAt().toString());
        }
    }

    private final Map<String, Seen> watermarks = new ConcurrentHashMap<>();

    public IngestResult check(TelemetryPoint point) {
        IngestResult[] result = new IngestResult[1];
        watermarks.compute(point.key(), (key, prev) -> {
            Seen incoming = Seen.of(point);
            if (prev == null || point.seq() > prev.seq()) {
                result[0] = new IngestResult.Accepted(point);
                return incoming;
            }
            if (point.seq() == prev.seq() && incoming.equals(prev)) {
                result[0] = new IngestResult.Duplicate(point);
                return prev;
            }
            result[0] = new IngestResult.Late(point, prev.seq());
            return prev;
        });
        log.debug("dedup check key={} seq={} -> {}", point.key(), point.seq(),
                result[0].getClass().getSimpleName());
        return result[0];
    }

    public void reset() {
        watermarks.clear();
    }
}
