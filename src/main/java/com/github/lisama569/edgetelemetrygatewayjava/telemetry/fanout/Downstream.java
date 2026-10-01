package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

/**
 * 下游注册信息：名字 + 通道。骨架。
 */
public record Downstream(String name, IdempotentSimulatedChannel channel) {
}
