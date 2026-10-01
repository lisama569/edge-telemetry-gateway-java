package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 本地持久化缓存：下游通道不可用时暂存数据，恢复后续传。
 *
 * 两种工作模式：
 * 1. 传统单出口模式（无注册消费者）：容量满时确定性淘汰最旧一条并计数；ack(count) 直接移除队首。
 * 2. 多下游模式（registerConsumer 注册过消费者）：每个下游各自维护已确认 offset 游标，
 *    只有全部下游都确认过的队首数据才物理移除；某下游慢/挂只拖住自己的游标，不影响其他下游。
 *    容量满且队首仍被任一下游占用时，确定性拒绝新写入（REJECTED_BUFFER_FULL），
 *    保证各下游收到的测点集合、顺序、条数完全一致。
 */
public interface TelemetryBuffer {

    /** 追加一条。返回追加结果（含分配的单调 offset；被拒绝时 offset 为 -1）。 */
    AppendOutcome append(TelemetryPoint point);

    /** 读取待转发批次（不删除）。传统单出口模式使用。 */
    List<TelemetryPoint> peekBatch(int max);

    /** 确认前 count 条已成功转发，从缓存中移除。传统单出口模式使用。 */
    void ack(int count);

    /** 注册一个下游消费者（幂等）；其游标从已落盘进度恢复，否则从 offset 起点之前开始。 */
    void registerConsumer(String consumer);

    /** 读取某消费者游标之后的待转发批次（不推进游标）。 */
    List<Envelope> peekBatchFor(String consumer, int max);

    /** 推进某消费者的已确认游标 count 条，并物理移除所有消费者都确认过的队首数据。 */
    void ack(String consumer, int count);

    /** 某消费者最近已确认的 offset（尚无确认时为 -1）。 */
    long ackedOffset(String consumer);

    long size();

    long droppedCount();

    /** 多下游模式下因容量满被确定性拒绝的写入条数。 */
    long rejectedFullCount();

    /** 带全局单调 offset 的信封：offset 是转发侧幂等/对账使用的稳定身份序号。 */
    record Envelope(long offset, TelemetryPoint point) {
    }

    enum AppendStatus {
        APPENDED,
        /** 传统单出口模式：容量已满，淘汰最旧一条后写入 */
        APPENDED_EVICTED_OLDEST,
        /** 多下游模式：容量已满且队首仍被慢下游占用，本次写入被确定性拒绝（不入队） */
        REJECTED_BUFFER_FULL
    }

    record AppendOutcome(AppendStatus status, TelemetryPoint evicted, long offset) {

        public static AppendOutcome appended(long offset) {
            return new AppendOutcome(AppendStatus.APPENDED, null, offset);
        }

        public static AppendOutcome evictedOldest(TelemetryPoint evicted, long offset) {
            return new AppendOutcome(AppendStatus.APPENDED_EVICTED_OLDEST, evicted, offset);
        }

        public static AppendOutcome rejectedFull() {
            return new AppendOutcome(AppendStatus.REJECTED_BUFFER_FULL, null, -1);
        }
    }
}
