package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 带幂等接收方的下游通道：投递后返回逐条判定。
 * 通道不可用时返回 null（或抛异常），调用方不得确认(ack)本批数据。
 */
public interface DownstreamChannel {

    /** 投递一个批次；通道不可用时返回 null，调用方不得 ack。 */
    DeliveryBatchResult deliver(List<TelemetryPoint> batch);

    boolean isUp();
}
