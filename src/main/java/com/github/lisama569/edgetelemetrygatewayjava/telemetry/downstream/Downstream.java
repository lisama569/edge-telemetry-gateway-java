package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;

/**
 * 一个下游：独立通道、独立转发循环、独立消费进度。
 *
 * 数据来源二选一：
 * - 双下游共享扇出：{@link #endpoint()} 非空，多个下游读取同一条共享入队序列，靠各自游标确认；
 * - 兼容单出口：{@link #buffer()} 非空，该下游独占一个“满则淘汰最旧”的独立缓存。
 */
public record Downstream(
        String name,
        DownstreamChannel channel,
        Forwarder forwarder,
        BufferEndpoint endpoint,
        TelemetryBuffer buffer) {

    /** 双下游共享扇出装配。 */
    public static Downstream shared(String name, DownstreamChannel channel,
                                    Forwarder forwarder, BufferEndpoint endpoint) {
        return new Downstream(name, channel, forwarder, endpoint, null);
    }

    /** 兼容原单出口装配（独立缓存，满则淘汰最旧）。 */
    public static Downstream standalone(String name, DownstreamChannel channel,
                                        Forwarder forwarder, TelemetryBuffer buffer) {
        return new Downstream(name, channel, forwarder, null, buffer);
    }

    public boolean isShared() {
        return endpoint != null;
    }

    /** 该下游尚未确认/仍在途的条数（对账用）。 */
    public long pending() {
        return endpoint != null ? endpoint.pending() : buffer.size();
    }
}
