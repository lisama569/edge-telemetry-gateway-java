package com.github.lisama569.edgetelemetrygatewayjava.config;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamFanOut;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.FileIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 下游拓扑装配。
 *
 * - 默认启用归档旁路（telemetry.archive.enabled=true）：cloud + archive 两个独立下游，
 *   各自拥有独立文件缓存（独立进度）、独立幂等身份账本（文件，跨重启保留）、独立转发线程；
 * - 关闭时退化为原单出口（仅 cloud），行为与上一版本兼容。
 */
@Configuration
public class DownstreamConfig {

    @Value("${telemetry.buffer.dir:./data/buffer}")
    private Path bufferDir;

    @Value("${telemetry.identity.dir:./data/identities}")
    private Path identityDir;

    @Value("${telemetry.buffer.capacity:10000}")
    private int capacity;

    @Value("${telemetry.forward.batch-size:100}")
    private int batchSize;

    @Value("${telemetry.forward.interval-millis:1000}")
    private long intervalMillis;

    @Value("${telemetry.archive.enabled:true}")
    private boolean archiveEnabled;

    @Value("${telemetry.archive.buffer.capacity:10000}")
    private int archiveCapacity;

    @Bean
    public DownstreamTopology downstreamTopology() {
        List<Downstream> list = new ArrayList<>();
        list.add(buildCloud());
        if (archiveEnabled) {
            list.add(buildArchive());
        }
        return new DownstreamTopology(list);
    }

    private Downstream buildCloud() {
        TelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, capacity);
        IdempotentReceiver receiver = new IdempotentReceiver(
                new FileIdentityStore(identityDir.resolve("cloud-identities.jsonl")));
        CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud", receiver);
        Forwarder forwarder = new Forwarder("cloud", buffer, channel, batchSize, intervalMillis);
        return new Downstream("cloud", buffer, channel, forwarder);
    }

    private Downstream buildArchive() {
        TelemetryBuffer buffer = FileTelemetryBuffer.at(bufferDir.resolve("archive-buffer.jsonl"), archiveCapacity);
        IdempotentReceiver receiver = new IdempotentReceiver(
                new FileIdentityStore(identityDir.resolve("archive-identities.jsonl")));
        ArchiveDownstreamChannel channel = new ArchiveDownstreamChannel("archive", receiver);
        Forwarder forwarder = new Forwarder("archive", buffer, channel, batchSize, intervalMillis);
        return new Downstream("archive", buffer, channel, forwarder);
    }

    @Bean
    public DownstreamFanOut downstreamFanOut(DownstreamTopology topology) {
        return new DownstreamFanOut(topology);
    }
}
