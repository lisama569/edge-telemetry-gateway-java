package com.github.lisama569.edgetelemetrygatewayjava.model;

/**
 * 单条报文进入网关后的处置结论。
 */
public record IngestResult(
        String outcome,
        RejectReason rejectReason,
        long entryId,
        String detail
) {
}
