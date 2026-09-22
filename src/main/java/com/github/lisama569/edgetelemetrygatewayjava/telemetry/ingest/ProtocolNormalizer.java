package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

/**
 * 协议归一器：将某种协议的原始报文解析为统一 TelemetryPoint。
 * 解析失败时抛出 NormalizationException 并携带可区分的拒绝原因。
 */
public interface ProtocolNormalizer {

    String protocol();

    TelemetryPoint normalize(RawFrame frame) throws NormalizationException;
}
