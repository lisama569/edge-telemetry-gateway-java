package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

/**
 * 下游对单条数据投递的判定结果，每种结论可单独识别、统计与查询。
 */
public enum DeliveryVerdict {
    /** 首次投递，下游真正接收（正常新数据） */
    ACCEPTED,
    /** 身份此前已接收过（如崩溃续传的重复投递），幂等丢弃，不真正接收 */
    DUPLICATE,
    /** 首次投递但 seq 落后于该测点水位（真实迟到数据），正常接收并标记 */
    LATE
}
