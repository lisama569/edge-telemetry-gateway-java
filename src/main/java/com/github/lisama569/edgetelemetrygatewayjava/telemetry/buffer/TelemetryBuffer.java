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

    /**
     * 历史上被确定性淘汰（溢出丢最旧）的条数。
     * 单出口兼容路径使用该语义；双下游共享准入日志只做“整条拒绝”，该值恒为 0。
     */
    long droppedCount();

    /**
     * 真正被准入（成功 append）的条数（进程内计数）。
     * 与“接收到的 + 仍缓存的 + 被淘汰的”对账，避免用反推值自证平衡。
     */
    long appendedCount();

    /**
     * 因容量满被整条拒绝、任何下游都没有准入的条数。
     * 仅双下游共享准入路径可能非 0；单出口淘汰路径该值恒为 0。
     */
    long rejectedCount();

    enum AppendStatus {
        APPENDED,
        /** 容量已满，淘汰最旧一条后写入（仅原单出口兼容路径） */
        APPENDED_EVICTED_OLDEST,
        /** 容量已满，整条拒绝、不写入（双下游路径：两侧一致地不收这条） */
        REJECTED_FULL
    }

    record AppendOutcome(AppendStatus status, TelemetryPoint evicted) {
    }
}
