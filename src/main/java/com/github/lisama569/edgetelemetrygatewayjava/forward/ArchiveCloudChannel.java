package com.github.lisama569.edgetelemetrygatewayjava.forward;

import org.springframework.stereotype.Component;

/**
 * 归档云端通道（本地模拟），代表第二个消费方。
 */
@Component
public class ArchiveCloudChannel extends AbstractSimulatedCloudChannel {

    public ArchiveCloudChannel() {
        super("archive");
    }
}
