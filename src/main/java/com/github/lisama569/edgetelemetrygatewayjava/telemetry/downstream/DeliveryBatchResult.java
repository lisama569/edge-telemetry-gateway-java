package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import java.util.List;

/**
 * 一个批次在某个下游的投递判定汇总。
 */
public record DeliveryBatchResult(List<DeliveryResult> results) {
}
