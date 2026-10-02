package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.FileSharedFanOutBuffer;

import java.util.List;

/**
 * 下游拓扑：固定顺序的下游集合（cloud 在前，archive 在后）。
 * 双下游共享扇出时，所有下游读取同一条共享入队序列（各自独立确认游标）。
 */
public class DownstreamTopology {

    private final List<Downstream> downstreams;
    /** 共享扇出队列；单出口装配为 null。 */
    private final FileSharedFanOutBuffer sharedBuffer;

    /** 双下游共享扇出装配。 */
    public DownstreamTopology(List<Downstream> downstreams, FileSharedFanOutBuffer sharedBuffer) {
        this.downstreams = List.copyOf(downstreams);
        this.sharedBuffer = sharedBuffer;
    }

    /** 兼容单出口/原有装配：无共享队列。 */
    public DownstreamTopology(List<Downstream> downstreams) {
        this(downstreams, null);
    }

    public List<Downstream> all() {
        return downstreams;
    }

    public Downstream get(String name) {
        for (Downstream d : downstreams) {
            if (d.name().equals(name)) {
                return d;
            }
        }
        throw new IllegalArgumentException("no such downstream: " + name);
    }

    /** 是否为“所有下游共享一条队列”的双下游装配。 */
    public boolean isSharedFanOut() {
        return sharedBuffer != null;
    }

    /**
     * 共享扇出缓存；单出口装配返回 null。
     */
    public FileSharedFanOutBuffer sharedFanOutBuffer() {
        return sharedBuffer;
    }
}
