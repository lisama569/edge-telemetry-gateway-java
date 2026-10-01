package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

/**
 * 本地归档下游通道（归档旁路）的模拟实现，与云端相互独立。
 */
public class ArchiveDownstreamChannel extends SimulatedDownstreamChannel {

    public ArchiveDownstreamChannel(String name, IdempotentReceiver receiver) {
        super(name, receiver);
    }
}
