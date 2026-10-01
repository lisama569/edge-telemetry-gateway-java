package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采集判定日志：每次注入（输入报文）与判定结论一一对应。
 *
 * - 每条记录分配唯一 decisionId（"d-000001"…），可通过 /api/decisions/{id} 单独识别查询；
 * - 有界保留最近 N 条，溢出淘汰最旧（仅内存态，本地验证用途）；
 * - verdict 归一为 ACCEPTED / DUPLICATE / LATE / REJECTED，
 *   REJECTED 额外给出具体 reason（UNKNOWN_PROTOCOL / MALFORMED_PAYLOAD / INVALID_FIELD / BUFFER_FULL），
 *   让"正常接收 / 重复 / 迟到 / 各种拒绝"在本地验证时无需靠猜。
 */
public class DecisionLog {

    private static final Logger log = LoggerFactory.getLogger(DecisionLog.class);

    private final int capacity;
    private final ConcurrentLinkedQueue<DecisionRecord> recent = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String, DecisionRecord> byId = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    public DecisionLog(int capacity) {
        this.capacity = capacity;
    }

    public DecisionRecord record(String protocol, String payload, IngestResult result) {
        String id = "d-" + String.format("%06d", sequence.incrementAndGet());
        String verdict;
        String reason = null;
        String detail = null;
        switch (result) {
            case IngestResult.Accepted a -> {
                verdict = "ACCEPTED";
                detail = "key=" + a.point().key() + " seq=" + a.point().seq();
            }
            case IngestResult.Duplicate d -> {
                verdict = "DUPLICATE";
                detail = "key=" + d.point().key() + " seq=" + d.point().seq()
                        + " (identical content, already watermarked)";
            }
            case IngestResult.Late l -> {
                verdict = "LATE";
                reason = "seq=" + l.point().seq() + " behind watermark " + l.lastSeenSeq()
                        + " for key=" + l.point().key();
                detail = "late/conflicting frame never overwrites accepted data";
            }
            case IngestResult.Rejected r -> {
                verdict = "REJECTED";
                reason = r.reason().name();
                detail = r.detail();
            }
        }
        DecisionRecord rec = new DecisionRecord(id, Instant.now(), protocol, payload, verdict, reason, detail);
        recent.add(rec);
        byId.put(id, rec);
        trimToCapacity();
        log.info("decision {} verdict={} reason={} protocol={} payload={}",
                id, verdict, reason, protocol, payload);
        return rec;
    }

    private void trimToCapacity() {
        while (recent.size() > capacity) {
            DecisionRecord oldest = recent.poll();
            if (oldest != null) {
                byId.remove(oldest.decisionId());
            }
        }
    }

    public Optional<DecisionRecord> find(String decisionId) {
        return Optional.ofNullable(byId.get(decisionId));
    }

    public List<DecisionRecord> recent(int limit) {
        List<DecisionRecord> all = new ArrayList<>(recent);
        int from = Math.max(0, all.size() - limit);
        return List.copyOf(all.subList(from, all.size()));
    }
}
