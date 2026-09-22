package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

/**
 * 采集归一结果：接受 / 重复 / 迟到 / 拒绝（含可区分原因）。
 */
public sealed interface IngestResult {

    record Accepted(TelemetryPoint point) implements IngestResult {
    }

    /** 与已接受报文内容一致的重发，直接丢弃不计数 */
    record Duplicate(TelemetryPoint point) implements IngestResult {
    }

    /** seq 落后于该测点已确认的最新序号，视为迟到，不覆盖已有数据 */
    record Late(TelemetryPoint point, long lastSeenSeq) implements IngestResult {
    }

    record Rejected(RejectReason reason, String detail) implements IngestResult {
    }
}
