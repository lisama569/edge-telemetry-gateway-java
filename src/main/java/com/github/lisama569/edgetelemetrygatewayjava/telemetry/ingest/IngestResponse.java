package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

/**
 * 控制面注入结果的可读 DTO：verdict 与本次输入一一对应，无需猜测即可区分
 * ACCEPTED / DUPLICATE / LATE / REJECTED 及具体拒绝原因。
 */
public record IngestResponse(
        String verdict,
        String sourceId,
        String pointId,
        Long seq,
        RejectReason reason,
        String detail,
        Long lastSeenSeq) {

    public static IngestResponse from(IngestResult result) {
        return switch (result) {
            case IngestResult.Accepted a -> of("ACCEPTED", a.point(), null, null, null);
            case IngestResult.Duplicate d -> of("DUPLICATE", d.point(), null, null, null);
            case IngestResult.Late l -> of("LATE", l.point(), null, null, l.lastSeenSeq());
            case IngestResult.Rejected r -> new IngestResponse(
                    "REJECTED", null, null, null, r.reason(), r.detail(), null);
        };
    }

    private static IngestResponse of(String verdict, TelemetryPoint p,
                                     RejectReason reason, String detail, Long lastSeenSeq) {
        return new IngestResponse(verdict, p.sourceId(), p.pointId(), p.seq(), reason, detail, lastSeenSeq);
    }
}
