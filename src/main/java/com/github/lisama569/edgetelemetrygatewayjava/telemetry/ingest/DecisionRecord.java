package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import java.time.Instant;

/**
 * 一次注入判定的完整记录骨架。
 */
public record DecisionRecord(
        String decisionId,
        Instant at,
        String protocol,
        String payload,
        String verdict,
        String reason,
        String detail) {
}
