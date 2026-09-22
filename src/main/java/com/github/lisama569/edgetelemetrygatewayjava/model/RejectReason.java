package com.github.lisama569.edgetelemetrygatewayjava.model;

/**
 * 报文被拒绝的可区分原因。
 */
public enum RejectReason {
    MALFORMED,
    MISSING_FIELD,
    BAD_VALUE,
    UNKNOWN_PROTOCOL,
    UNKNOWN_SOURCE,
    DUPLICATE,
    STALE,
    BACKPRESSURE_REJECTED
}
