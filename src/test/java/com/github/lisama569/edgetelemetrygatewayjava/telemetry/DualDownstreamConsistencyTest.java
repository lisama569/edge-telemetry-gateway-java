package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamFanOut;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.InMemoryIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Reconciliation;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.SimulatedDownstreamChannel;
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
 * 归档旁路双下游一致性对账：
 * - 同一份数据进入 cloud 与 archive，全部 drain 后两边按测点的序号序列逐条一致（条数+顺序）；
 * - 一个下游变慢/失败只阻塞自身，不拖另一个，也不会让另一个多收/漏收；
 * - 缓存容量到上限时确定性淘汰最旧，新下游也纳入对账（appended == received + buffered + dropped）；
 * - 恢复后各按自身进度续传，重复 flush 不重发已确认数据。
 */
class DualDownstreamConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamConsistencyTest.class);

    @TempDir
    Path tempDir;

    private record Pair(DownstreamTopology topology,
                        DownstreamFanOut fanOut,
                        CloudDownstreamChannel cloud,
                        ArchiveDownstreamChannel archive) {
    }

    private Pair topology(int cloudCapacity, int archiveCapacity) {
        TelemetryBuffer cloudBuffer = FileTelemetryBuffer.at(tempDir.resolve("cloud.jsonl"), cloudCapacity);
        TelemetryBuffer archiveBuffer = FileTelemetryBuffer.at(tempDir.resolve("archive.jsonl"), archiveCapacity);
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder cloudFwd = new Forwarder("cloud", cloudBuffer, cloud, 100, 1000);
        Forwarder archiveFwd = new Forwarder("archive", archiveBuffer, archive, 100, 1000);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudBuffer, cloud, cloudFwd),
                new Downstream("archive", archiveBuffer, archive, archiveFwd)));
        return new Pair(topology, new DownstreamFanOut(topology), cloud, archive);
    }

    private void ingest(DownstreamFanOut fanOut, long from, long to) {
        for (long seq = from; seq <= to; seq++) {
            fanOut.append(new TelemetryPoint("plc-1", "temperature", seq,
                    Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD));
        }
    }

    private void drain(Downstream d) {
        int n;
        do {
            n = d.forwarder().flushOnce();
        } while (n > 0);
    }

    private void logReconciliation(DownstreamTopology topology) {
        for (Downstream d : topology.all()) {
            log.info("reconciliation[{}]: {}", d.name(), Reconciliation.snapshot(d));
        }
    }

    @Test
    void bothDownstreamsReceiveIdenticalPointSequences() {
        Pair p = topology(100, 100);
        ingest(p.fanOut(), 1, 10);
        drain(p.topology().get("cloud"));
        drain(p.topology().get("archive"));

        List<TelemetryPoint> cloudReceived = p.cloud().received();
        List<TelemetryPoint> archiveReceived = p.archive().received();
        log.info("cloud seqs={}", cloudReceived.stream().map(TelemetryPoint::seq).toList());
        log.info("archive seqs={}", archiveReceived.stream().map(TelemetryPoint::seq).toList());

        assertThat(archiveReceived).hasSameSizeAs(cloudReceived);
        assertThat(archiveReceived).extracting(TelemetryPoint::seq)
                .containsExactlyElementsOf(cloudReceived.stream().map(TelemetryPoint::seq).toList());
        assertThat(Reconciliation.diffByPoint(cloudReceived, archiveReceived)).isEmpty();
        logReconciliation(p.topology());
        for (Downstream d : p.topology().all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            assertThat(Reconciliation.balanced(10, ch.received().size(), d.buffer().size(),
                    d.buffer().droppedCount())).isTrue();
        }
    }

    @Test
    void archiveDownDoesNotBlockCloudAndCloudResumesIndependently() {
        Pair p = topology(100, 100);
        // 归档通道故障
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 6);

        // 云端照常 drain，归档一轮不确认、数据保留在自己的缓存
        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).hasSize(6);
        assertThat(p.topology().get("archive").buffer().size()).isEqualTo(6);
        assertThat(p.archive().received()).isEmpty();
        log.info("archive down: cloudReceived={} archiveBuffered={}",
                p.cloud().received().size(), p.topology().get("archive").buffer().size());

        // 归档恢复后按自己的进度续传，最终两边一致
        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        logReconciliation(p.topology());
    }

    @Test
    void slowArchiveDoesNotSlowCloudDown() {
        Pair p = topology(100, 100);
        p.archive().setLatencyMillis(50); // 归档变慢
        ingest(p.fanOut(), 1, 5);

        long start = System.currentTimeMillis();
        int cloudConfirmed = 0;
        int n;
        while ((n = p.topology().get("cloud").forwarder().flushOnce()) > 0) {
            cloudConfirmed += n;
        }
        long cloudElapsed = System.currentTimeMillis() - start;
        assertThat(cloudConfirmed).isEqualTo(5);
        // 云端完全不等待归档（远小于归档 5 条 * 50ms 的串行耗时）
        assertThat(cloudElapsed).isLessThan(200);
        log.info("cloud drained independently of slow archive in {}ms; archiveBuffered={}",
                cloudElapsed, p.topology().get("archive").buffer().size());

        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).hasSize(5);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
    }

    @Test
    void archiveOverflowIsDeterministicAndIncludedInReconciliation() {
        Pair p = topology(100, 3); // 归档容量仅 3
        ingest(p.fanOut(), 1, 5);

        TelemetryBuffer archiveBuffer = p.topology().get("archive").buffer();
        // 归档确定性淘汰最旧 seq=1,2，保留 3,4,5；云端不受影响
        assertThat(archiveBuffer.droppedCount()).isEqualTo(2);
        assertThat(archiveBuffer.peekBatch(10)).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L);
        assertThat(p.topology().get("cloud").buffer().droppedCount()).isZero();
        log.info("archive overflow: dropped={} retained={}", archiveBuffer.droppedCount(),
                archiveBuffer.peekBatch(10).stream().map(TelemetryPoint::seq).toList());

        drain(p.topology().get("cloud"));
        drain(p.topology().get("archive"));

        // 新下游纳入对账：归档 appended=5 == received 3 + buffered 0 + dropped 2
        SimulatedDownstreamChannel archive = p.archive();
        assertThat(Reconciliation.balanced(5, archive.received().size(),
                archiveBuffer.size(), archiveBuffer.droppedCount())).isTrue();
        // 云端仍是完整 5 条，不受归档溢出影响
        assertThat(p.cloud().received()).hasSize(5);
        assertThat(archive.received()).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L);
        logReconciliation(p.topology());

        // 重复触发 flush：已确认数据不重发
        assertThat(p.topology().get("cloud").forwarder().flushOnce()).isZero();
        assertThat(p.topology().get("archive").forwarder().flushOnce()).isZero();
        assertThat(p.cloud().received()).hasSize(5);
        assertThat(p.archive().received()).hasSize(3);
    }

    @Test
    void eachDownstreamResumesFromItsOwnProgressAndRepeatedFlushDoesNotResend() {
        // 云端批次=2，归档批次=100，制造两边不同的消费进度
        TelemetryBuffer cloudBuffer = FileTelemetryBuffer.at(tempDir.resolve("cprog.jsonl"), 100);
        TelemetryBuffer archiveBuffer = FileTelemetryBuffer.at(tempDir.resolve("aprog.jsonl"), 100);
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder cloudFwd = new Forwarder("cloud", cloudBuffer, cloud, 2, 1000);
        Forwarder archiveFwd = new Forwarder("archive", archiveBuffer, archive, 100, 1000);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudBuffer, cloud, cloudFwd),
                new Downstream("archive", archiveBuffer, archive, archiveFwd)));
        DownstreamFanOut fanOut = new DownstreamFanOut(topology);
        ingest(fanOut, 1, 5);

        // 云端消费一轮（2 条）；归档整轮停摆
        archive.bringDown();
        assertThat(cloudFwd.flushOnce()).isEqualTo(2);
        assertThat(archiveFwd.flushOnce()).isZero();
        assertThat(cloudBuffer.size()).isEqualTo(3);
        assertThat(archiveBuffer.size()).isEqualTo(5);
        log.info("diverged progress: cloudBuffered={} archiveBuffered={}",
                cloudBuffer.size(), archiveBuffer.size());

        // 云端继续 drain 到空，重复触发不重发
        drain(topology.get("cloud"));
        assertThat(cloudFwd.flushOnce()).isZero();
        assertThat(cloud.received()).hasSize(5);
        assertThat(cloud.receiver().duplicateCount()).isZero();

        // 归档恢复后按自己的完整进度续传 5 条，最终两边逐条一致
        archive.bringUp();
        drain(topology.get("archive"));
        assertThat(archive.received()).hasSize(5);
        assertThat(Reconciliation.diffByPoint(cloud.received(), archive.received())).isEmpty();
        log.info("independent progress resume: cloud={} archive={}",
                cloud.received().size(), archive.received().size());
    }
}
