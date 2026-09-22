package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 本地持久化缓存：云端通道不可用时暂存数据，恢复后续传。
 * 语义：容量有限，溢出策略确定（丢弃最旧并计数）；确认(ack)后删除，重启不丢未确认数据。
 */
public interface TelemetryBuffer {

    /** 追加一条。返回追加结果（成功或因溢出淘汰了最旧数据）。 */
    AppendOutcome append(TelemetryPoint point);

    /** 读取待转发批次（不删除）。 */
    List<TelemetryPoint> peekBatch(int max);

    /** 确认前 count 条已成功转发，从缓存中移除。 */
    void ack(int count);

    long size();

    long droppedCount();

    enum AppendStatus {
        APPENDED,
        /** 容量已满，淘汰最旧一条后写入 */
        APPENDED_EVICTED_OLDEST
    }

    record AppendOutcome(AppendStatus status, TelemetryPoint evicted) {
    }
}
