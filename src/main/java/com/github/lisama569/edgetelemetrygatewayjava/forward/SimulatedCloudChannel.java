package com.github.lisama569.edgetelemetrygatewayjava.forward;

import org.springframework.stereotype.Component;

/**
 * 主云端通道（本地模拟）。
 */
@Component
public class SimulatedCloudChannel extends AbstractSimulatedCloudChannel {

    public SimulatedCloudChannel() {
        super("primary");
    }
}
