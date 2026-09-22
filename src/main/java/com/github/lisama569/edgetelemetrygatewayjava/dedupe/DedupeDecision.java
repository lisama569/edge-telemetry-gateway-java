package com.github.lisama569.edgetelemetrygatewayjava.dedupe;

/**
 * 同一 (sourceId, pointId) 测点的乱序/重复判定。
 */
public enum DedupeDecision {
    ACCEPT,
    DUPLICATE,
    STALE
}
