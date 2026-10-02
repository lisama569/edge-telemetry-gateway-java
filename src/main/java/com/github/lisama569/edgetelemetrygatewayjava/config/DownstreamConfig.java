package com.github.lisama569.edgetelemetrygatewayjava.config;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.FileSharedFanOutBuffer;
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
 * - 默认启用归档旁路（telemetry.archive.enabled=true）：cloud + archive 共享一条持久化队列
 *   {@link FileSharedFanOutBuffer}，同一份数据只入队一次，两边以各自确认游标消费同一条序列，
 *   因此单侧故障/变慢 + 写满都不会造成两侧分叉；各下游仍有独立通道、独立幂等账本、独立转发线程。
 * - 关闭时退化为原单出口（仅 cloud），使用独立 {@link FileTelemetryBuffer}，满则淘汰最旧，
 *   行为与原单出口版本兼容。
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

    @Bean
    public DownstreamTopology downstreamTopology() {
        List<Downstream> list = new ArrayList<>();
        if (archiveEnabled) {
            FileSharedFanOutBuffer shared =
                    new FileSharedFanOutBuffer(bufferDir.resolve("fanout"), capacity);
            BufferEndpoint cloudEndpoint = shared.subscribe("cloud");
            BufferEndpoint archiveEndpoint = shared.subscribe("archive");

            CloudDownstreamChannel cloudChannel = new CloudDownstreamChannel("cloud",
                    new IdempotentReceiver(new FileIdentityStore(identityDir.resolve("cloud-identities.jsonl"))));
            ArchiveDownstreamChannel archiveChannel = new ArchiveDownstreamChannel("archive",
                    new IdempotentReceiver(new FileIdentityStore(identityDir.resolve("archive-identities.jsonl"))));

            Forwarder cloudForwarder = new Forwarder("cloud", cloudEndpoint, cloudChannel, batchSize, intervalMillis);
            Forwarder archiveForwarder = new Forwarder("archive", archiveEndpoint, archiveChannel, batchSize, intervalMillis);

            list.add(Downstream.shared("cloud", cloudChannel, cloudForwarder, cloudEndpoint));
            list.add(Downstream.shared("archive", archiveChannel, archiveForwarder, archiveEndpoint));
            return new DownstreamTopology(list, shared);
        } else {
            TelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, capacity);
            CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud",
                    new IdempotentReceiver(new FileIdentityStore(identityDir.resolve("cloud-identities.jsonl"))));
            Forwarder forwarder = new Forwarder("cloud", buffer, channel, batchSize, intervalMillis);
            list.add(Downstream.standalone("cloud", channel, forwarder, buffer));
            return new DownstreamTopology(list);
        }
    }

    @Bean
    public DownstreamFanOut downstreamFanOut(DownstreamTopology topology) {
        // 双下游：全部下游共享同一队列；单出口：直接写 cloud 的独立缓存。
        if (topology.isSharedFanOut()) {
            return new DownstreamFanOut(topology.sharedFanOutBuffer());
        }
        return new DownstreamFanOut(topology.all().get(0).buffer());
    }
}
