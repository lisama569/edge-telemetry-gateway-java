package com.github.lisama569.edgetelemetrygatewayjava.telemetry.model;

/**
 * 现场侧到达的原始报文。protocol 标识协议类型，payload 为协议原始负载（文本模拟）。
 */
public record RawFrame(String protocol, String payload) {
}
