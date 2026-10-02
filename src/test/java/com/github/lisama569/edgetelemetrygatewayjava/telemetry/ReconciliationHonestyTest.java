package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.InMemoryIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Reconciliation;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 对账如实性：旧实现用 received+buffered+dropped 反推 appended，balanced 永远为 true，
 * 即使两个下游已经分叉也报“平衡/正常”。这里固化新对账必须真实暴露：
 * - 两侧逐测点序列不一致（一边有某序号、另一边没有）时 crossDownstreamDiffs 非空；
 * - 真实 admitted 与 received+buffered+evicted 不等时 balanced=false（不允许自证平衡）。
 */
class ReconciliationHonestyTest {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationHonestyTest.class);

    @TempDir
    Path tempDir;

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    private Downstream downstream(String name, String file) {
        TelemetryBuffer buffer = FileTelemetryBuffer.at(tempDir.resolve(file), 100);
        var channel = name.equals("cloud")
                ? new CloudDownstreamChannel(name, new IdempotentReceiver(new InMemoryIdentityStore()))
                : new ArchiveDownstreamChannel(name, new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder fwd = new Forwarder(name, buffer, channel, 100, 1000);
        return new Downstream(name, buffer, channel, fwd);
    }

    @Test
    void fabricatedAppendedCanNoLongerHideImbalance() {
        log.info("[对账] 旧的反推算法：appended=received+buffered+dropped 恒平衡，发现不了问题");
        // 模拟“真实准入 10 条，但下游只真正收到 8 条、缓存空、淘汰 0”——数据对不上
        long realAdmitted = 10;
        long received = 8;
        long buffered = 0;
        long evicted = 0;

        // 新对账使用真实 admitted：10 != 8+0+0 -> 不平衡，必须暴露
        assertThat(Reconciliation.balanced(realAdmitted, received, buffered, evicted))
                .as("real counters must expose the gap").isFalse();
        // 只有数字真的对得上时才平衡
        assertThat(Reconciliation.balanced(10, 8, 1, 1)).isTrue();
        log.info("balanced(10,8,0,0)=false 与 balanced(10,8,1,1)=true，对账不再自证平衡");
    }

    @Test
    void crossDownstreamDiffsDetectsOneSidedMissingSequence() {
        log.info("[对账] 一侧缺序号（典型分叉）：crossDownstreamDiffs 必须非空并指出测点");
        Downstream cloud = downstream("cloud", "c.jsonl");
        Downstream archive = downstream("archive", "a.jsonl");

        // cloud 真正收到 1..5；archive 因单侧故障+单侧淘汰只收到 1,3,4,5（缺 2）
        for (long seq : List.of(1L, 2L, 3L, 4L, 5L)) {
            cloud.buffer().append(point(seq));
        }
        for (long seq : List.of(1L, 3L, 4L, 5L)) {
            archive.buffer().append(point(seq));
        }
        new Forwarder("cloud", cloud.buffer(),
                (com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamChannel)
                        cloud.channel(), 100, 1000).flushOnce();
        new Forwarder("archive", archive.buffer(),
                (com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamChannel)
                        archive.channel(), 100, 1000).flushOnce();

        List<String> diffs = Reconciliation.crossDownstreamDiffs(cloud, archive);
        log.info("detected diffs: {}", diffs);
        assertThat(diffs).hasSize(1);
        assertThat(diffs.get(0)).contains("plc-1/temperature");
        assertThat(diffs.get(0)).contains("left(count=5)").contains("right(count=4)");

        // 即使各自内部数字“看起来平衡”，跨下游分叉仍被如实暴露
        var cloudCh = (com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream
                .SimulatedDownstreamChannel) cloud.channel();
        var archiveCh = (com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream
                .SimulatedDownstreamChannel) archive.channel();
        assertThat(Reconciliation.balanced(cloud.buffer().appendedCount(), cloudCh.received().size(),
                cloud.buffer().size(), cloud.buffer().droppedCount())).isTrue();
        assertThat(Reconciliation.balanced(archive.buffer().appendedCount(), archiveCh.received().size(),
                archive.buffer().size(), archive.buffer().droppedCount())).isTrue();
        log.info("both sides self-balanced yet cross-diff non-empty: the old false 'normal' is now exposed");
    }

    @Test
    void identicalSequencesProduceNoDiffs() {
        log.info("[对账] 两侧序列一致：diffs 为空");
        Downstream cloud = downstream("cloud", "c2.jsonl");
        Downstream archive = downstream("archive", "a2.jsonl");
        for (long seq = 1; seq <= 4; seq++) {
            cloud.buffer().append(point(seq));
            archive.buffer().append(point(seq));
        }
        assertThat(Reconciliation.crossDownstreamDiffs(cloud, archive)).isEmpty();
    }
}
