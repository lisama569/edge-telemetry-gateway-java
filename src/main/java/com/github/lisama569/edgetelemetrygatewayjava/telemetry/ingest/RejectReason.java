package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

/**
 * 报文被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 协议类型未注册/不识别 */
    UNKNOWN_PROTOCOL,
    /** 报文负载无法按协议解析（格式错误、缺字段） */
    MALFORMED_PAYLOAD,
    /** 数值或时间戳等字段取值非法 */
    INVALID_FIELD
}
