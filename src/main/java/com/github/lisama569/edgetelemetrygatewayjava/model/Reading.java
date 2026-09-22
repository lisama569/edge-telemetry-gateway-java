package com.github.lisama569.edgetelemetrygatewayjava.model;

/**
 * 协议归一后的标准测点读数。
 */
public record Reading(
        String sourceId,
        String pointId,
        long seq,
        long timestampMs,
        double value
) {
}
