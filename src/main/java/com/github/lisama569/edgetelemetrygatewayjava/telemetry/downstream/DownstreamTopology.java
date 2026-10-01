package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import java.util.List;

/**
 * 下游拓扑：固定顺序的独立下游集合（cloud 在前，archive 在后）。
 * 顺序用于采集侧扇出，保证两份数据以相同入队顺序进入各自缓存。
 */
public class DownstreamTopology {

    private final List<Downstream> downstreams;

    public DownstreamTopology(List<Downstream> downstreams) {
        this.downstreams = List.copyOf(downstreams);
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
}
