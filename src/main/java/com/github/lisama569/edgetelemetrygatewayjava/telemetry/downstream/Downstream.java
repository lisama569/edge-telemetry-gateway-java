package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;

/**
 * 一个独立下游：独立缓存进度与独立转发循环，彼此不拖后腿。
 */
public record Downstream(
        String name,
        TelemetryBuffer buffer,
        DownstreamChannel channel,
        Forwarder forwarder) {
}
