package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 云端通道抽象。send 成功返回 true；通道不可用时返回 false 或抛 ChannelUnavailableException，
 * 调用方不得确认(ack)未成功的数据。
 */
public interface CloudChannel {

    boolean send(List<TelemetryPoint> batch);

    boolean isUp();
}
