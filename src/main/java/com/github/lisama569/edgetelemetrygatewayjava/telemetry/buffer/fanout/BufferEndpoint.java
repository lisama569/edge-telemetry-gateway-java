package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 共享扇出缓存上的“某一个下游消费者”视图。
 *
 * 每个下游拥有自己的独立确认游标：从共享队首按自己的进度读取与确认，
 * 不淘汰任何数据，也不影响另一个下游看到的序列。
 */
public interface BufferEndpoint {

    /** 读取该消费者尚未确认的队首批次（只读，不删除）。 */
    List<TelemetryPoint> peekBatch(int max);

    /** 确认本消费者已成功接收前 count 条，仅推进自己的游标。 */
    void ack(int count);

    /** 该消费者尚未确认的条数。 */
    long pending();

    /** 该消费者累计已确认条数。 */
    long ackedTotal();

    /** 该消费者名称（用于日志/对账）。 */
    String name();
}
