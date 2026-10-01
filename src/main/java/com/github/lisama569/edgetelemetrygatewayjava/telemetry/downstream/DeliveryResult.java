package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;

/**
 * 单条数据在某个下游的投递判定：身份与结论一一对应，便于本地核对。
 */
public record DeliveryResult(PointIdentity identity, DeliveryVerdict verdict) {
}
