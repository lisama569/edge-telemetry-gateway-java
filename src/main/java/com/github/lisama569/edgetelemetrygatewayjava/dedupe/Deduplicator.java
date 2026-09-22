package com.github.lisama569.edgetelemetrygatewayjava.dedupe;

import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 按测点 (sourceId,pointId) 维护已接受的最大序号。
 *
 * 判定规则（以现场数据源单调递增的 seq 为准，与到达时间无关）：
 * - seq 大于该测点已接受最大序号：ACCEPT，并预留该序号；
 * - seq 等于已接受最大序号：DUPLICATE（重复报文，不重复计数）；
 * - seq 小于已接受最大序号：STALE（乱序/迟到的旧报文，不允许覆盖新值）。
 *
 * 预留与提交分离：ACCEPT 仅完成“预留”，待数据成功落盘后才算提交；
 * 若落盘因背压失败，IngestService 必须调用 release 回滚预留，
 * 否则上游重试同一报文会被误判为重复而丢失。release 使用
 * BiFunction 条件回滚，避免覆盖并发到达的更大序号。
 */
@Component
public class Deduplicator {

    private static final Logger log = LoggerFactory.getLogger(Deduplicator.class);

    private final Map<String, Long> latestSeq = new ConcurrentHashMap<>();

    private static String key(Reading r) {
        return r.sourceId() + "|" + r.pointId();
    }

    public static String key(String sourceId, String pointId) {
        return sourceId + "|" + pointId;
    }

    public DedupeResult decide(Reading reading) {
        String key = key(reading);
        long[] previous = {-1L};
        DedupeDecision[] decision = {DedupeDecision.STALE};
        latestSeq.compute(key, (k, current) -> {
            previous[0] = current == null ? -1L : current;
            if (current == null || reading.seq() > current) {
                decision[0] = DedupeDecision.ACCEPT;
                return reading.seq();
            }
            decision[0] = reading.seq() == current ? DedupeDecision.DUPLICATE : DedupeDecision.STALE;
            return current;
        });
        log.info("[dedupe] 测点={} seq={} 判定前最大序号={} 判定={}",
                key, reading.seq(), previous[0], decision[0]);
        return new DedupeResult(decision[0], previous[0]);
    }

    /**
     * 回滚一次 ACCEPT 预留。仅当当前最大序号仍等于本次预留的 seq 时回退，
     * 防止覆盖并发已接受的更大序号；previousSeq == -1 表示恢复为“无记录”。
     */
    public void release(Reading reading, long previousSeq) {
        String key = key(reading);
        latestSeq.computeIfPresent(key, (k, current) -> {
            if (current.longValue() != reading.seq()) {
                return current;
            }
            log.info("[dedupe] 回滚预留 测点={} seq={} -> 旧最大序号={}（落盘失败，允许重试）",
                    key, reading.seq(), previousSeq);
            return previousSeq == -1L ? null : previousSeq;
        });
    }

    public long acceptedSequenceOf(String sourceId, String pointId) {
        return latestSeq.getOrDefault(key(sourceId, pointId), -1L);
    }

    public void reset() {
        latestSeq.clear();
        log.info("[dedupe] 序号状态已重置");
    }
}
