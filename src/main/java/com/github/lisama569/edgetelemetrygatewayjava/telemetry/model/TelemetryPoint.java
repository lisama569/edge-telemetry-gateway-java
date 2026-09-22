package com.github.lisama569.edgetelemetrygatewayjava.telemetry.model;

import java.time.Instant;

/**
 * 归一化后的遥测点。同一测点由 (sourceId, pointId) 唯一标识，
 * seq 为来源侧单调递增序号，用于乱序/重复/迟到判定。
 */
public record TelemetryPoint(
        String sourceId,
        String pointId,
        long seq,
        Instant sampledAt,
        double value,
        Quality quality) {

    public enum Quality {
        GOOD, BAD, UNCERTAIN
    }

    public String key() {
        return sourceId + "/" + pointId;
    }
}
