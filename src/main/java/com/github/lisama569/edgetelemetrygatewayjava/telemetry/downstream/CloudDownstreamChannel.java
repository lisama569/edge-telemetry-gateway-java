package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

/**
 * 云端下游通道（原单出口）的模拟实现。
 */
public class CloudDownstreamChannel extends SimulatedDownstreamChannel {

    public CloudDownstreamChannel(String name, IdempotentReceiver receiver) {
        super(name, receiver);
    }
}
