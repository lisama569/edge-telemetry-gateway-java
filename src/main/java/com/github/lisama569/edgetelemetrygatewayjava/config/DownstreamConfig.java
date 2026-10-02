package com.github.lisama569.edgetelemetrygatewayjava.config;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.SharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamFanOut;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.FileIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.SimulatedDownstreamChannel;
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
 * - 默认启用归档旁路（telemetry.archive.enabled=true）：cloud + archive 共享<strong>一条</strong>
 *   准入日志（{@link SharedFanOutBuffer}），每个下游只持有独立投递游标、独立幂等身份账本
 *   （文件，跨重启保留）与独立转发线程：单侧 down/变慢不拖另一侧；共享容量满时整条数据
 *   被两侧一致拒绝，绝不出现在单侧悄悄淘汰最旧导致的分叉。
 * - 关闭时退化为原单出口（仅 cloud，独立 {@link FileTelemetryBuffer}，满则淘汰最旧），
 *   行为与更早版本兼容。
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

    /** 共享准入日志容量；缺省与云端容量一致。 */
    @Value("${telemetry.fanout.capacity:10000}")
    private int fanOutCapacity;

    /** 双下游共享准入日志；单出口模式下不创建。 */
    @Bean
    public SharedFanOutBuffer sharedFanOutBuffer() {
        if (!archiveEnabled) {
            return null;
        }
        int sharedCapacity = Math.min(capacity, fanOutCapacity);
        return SharedFanOutBuffer.create(
                bufferDir.resolve("fanout"), sharedCapacity, List.of("cloud", "archive"));
    }

    @Bean
    public DownstreamTopology downstreamTopology(SharedFanOutBuffer shared) {
        if (!archiveEnabled || shared == null) {
            return new DownstreamTopology(List.of(buildSingleCloud()));
        }
        List<Downstream> list = new ArrayList<>();
        list.add(buildFromView("cloud", shared.view("cloud"), CloudDownstreamChannel::new));
        list.add(buildFromView("archive", shared.view("archive"), ArchiveDownstreamChannel::new));
        return new DownstreamTopology(list);
    }

    private Downstream buildSingleCloud() {
        TelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, capacity);
        IdempotentReceiver receiver = new IdempotentReceiver(
                new FileIdentityStore(identityDir.resolve("cloud-identities.jsonl")));
        CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud", receiver);
        Forwarder forwarder = new Forwarder("cloud", buffer, channel, batchSize, intervalMillis);
        return new Downstream("cloud", buffer, channel, forwarder);
    }

    private Downstream buildFromView(String name,
                                     TelemetryBuffer view,
                                     ChannelFactory factory) {
        IdempotentReceiver receiver = new IdempotentReceiver(
                new FileIdentityStore(identityDir.resolve(name + "-identities.jsonl")));
        SimulatedDownstreamChannel channel = factory.create(name, receiver);
        Forwarder forwarder = new Forwarder(name, view, channel, batchSize, intervalMillis);
        return new Downstream(name, view, channel, forwarder);
    }

    @FunctionalInterface
    private interface ChannelFactory {
        SimulatedDownstreamChannel create(String name, IdempotentReceiver receiver);
    }

    @Bean
    public DownstreamFanOut downstreamFanOut(DownstreamTopology topology,
                                             SharedFanOutBuffer shared) {
        if (!archiveEnabled || shared == null) {
            return new DownstreamFanOut(topology.get("cloud").buffer());
        }
        return new DownstreamFanOut(shared, shared.names());
    }
}
