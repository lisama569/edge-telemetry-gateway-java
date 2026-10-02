package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.FileSharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamFanOut;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
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
 * 共享扇出队列下的双下游一致性对账：
 * - 同一份数据只入共享队列一次，全部 drain 后两边按测点的序号序列逐条一致（条数+顺序）；
 * - 一个下游 down/变慢只堆积属于它的未确认条目，不拖另一个，也不会让另一个多收/漏收；
 * - 共享容量到上限时确定性拒绝写入（背压，可重试），不淘汰任何已入队数据，两边不产生分叉；
 * - 恢复后各按自身游标续传；重复 flush 不重发已确认数据；满时被拒的写入重试后成功。
 */
class DualDownstreamConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamConsistencyTest.class);

    @TempDir
    Path tempDir;

    private record Pair(DownstreamTopology topology,
                        DownstreamFanOut fanOut,
                        FileSharedFanOutBuffer shared,
                        BufferEndpoint cloudEndpoint,
                        BufferEndpoint archiveEndpoint,
                        CloudDownstreamChannel cloud,
                        ArchiveDownstreamChannel archive) {
    }

    private Pair topology(int capacity, int cloudBatch, int archiveBatch) {
        FileSharedFanOutBuffer shared =
                new FileSharedFanOutBuffer(tempDir.resolve("fanout-" + capacity), capacity);
        BufferEndpoint cloudEndpoint = shared.subscribe("cloud");
        BufferEndpoint archiveEndpoint = shared.subscribe("archive");
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder cloudFwd = new Forwarder("cloud", cloudEndpoint, cloud, cloudBatch, 1000);
        Forwarder archiveFwd = new Forwarder("archive", archiveEndpoint, archive, archiveBatch, 1000);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                Downstream.shared("cloud", cloud, cloudFwd, cloudEndpoint),
                Downstream.shared("archive", archive, archiveFwd, archiveEndpoint)), shared);
        return new Pair(topology, new DownstreamFanOut(shared), shared,
                cloudEndpoint, archiveEndpoint, cloud, archive);
    }

    private Pair topology(int capacity) {
        return topology(capacity, 100, 100);
    }

    private void ingest(DownstreamFanOut fanOut, long from, long to) {
        for (long seq = from; seq <= to; seq++) {
            fanOut.append(point(seq));
        }
    }

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    private void drain(Downstream d) {
        int n;
        do {
            n = d.forwarder().flushOnce();
        } while (n > 0);
    }

    private void logState(String phase, Pair p) {
        log.info("{}: appendedTotal={} sharedPending={} rejectedFull={} cloudPending={} archivePending={} "
                        + "cloudReceived={} archiveReceived={}",
                phase, p.shared().appendedTotal(), p.shared().size(), p.shared().rejectedFullCount(),
                p.cloudEndpoint().pending(), p.archiveEndpoint().pending(),
                p.cloud().received().size(), p.archive().received().size());
    }

    @Test
    void bothDownstreamsReceiveIdenticalPointSequences() {
        Pair p = topology(100);
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
        assertThat(p.shared().size()).isZero();
        logState("drained", p);
    }

    @Test
    void archiveDownDoesNotBlockCloudAndCloudResumesIndependently() {
        Pair p = topology(100);
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 6);

        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).hasSize(6);
        // archive 停摆：6 条全部仍由它占用，共享队列无法裁剪；archive 一条未收
        assertThat(p.archiveEndpoint().pending()).isEqualTo(6);
        assertThat(p.cloudEndpoint().pending()).isZero();
        assertThat(p.shared().size()).isEqualTo(6);
        assertThat(p.archive().received()).isEmpty();
        logState("archive-down/cloud-drained", p);

        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertThat(p.shared().size()).isZero();
        logState("archive-recovered", p);
    }

    @Test
    void slowArchiveDoesNotSlowCloudDown() {
        Pair p = topology(100);
        p.archive().setLatencyMillis(50);
        ingest(p.fanOut(), 1, 5);

        long start = System.currentTimeMillis();
        int cloudConfirmed = 0;
        int n;
        while ((n = p.topology().get("cloud").forwarder().flushOnce()) > 0) {
            cloudConfirmed += n;
        }
        long cloudElapsed = System.currentTimeMillis() - start;
        assertThat(cloudConfirmed).isEqualTo(5);
        assertThat(cloudElapsed).isLessThan(200);
        log.info("cloud drained independently of slow archive in {}ms; archivePending={}",
                cloudElapsed, p.archiveEndpoint().pending());

        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).hasSize(5);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
    }

    @Test
    void sharedBufferFullRejectsDeterministicallyAndNeverDivergesSides() {
        // 容量仅 3，archive 停摆：最多接受 3 条入队，第 4、5 条被确定性拒绝（背压），不淘汰
        Pair p = topology(3);
        p.archive().bringDown();

        assertThat(p.fanOut().append(point(1))).isEqualTo(DownstreamFanOut.AppendOutcome.APPENDED);
        assertThat(p.fanOut().append(point(2))).isEqualTo(DownstreamFanOut.AppendOutcome.APPENDED);
        assertThat(p.fanOut().append(point(3))).isEqualTo(DownstreamFanOut.AppendOutcome.APPENDED);
        assertThat(p.fanOut().append(point(4))).isEqualTo(DownstreamFanOut.AppendOutcome.REJECTED_FULL);
        assertThat(p.fanOut().append(point(5))).isEqualTo(DownstreamFanOut.AppendOutcome.REJECTED_FULL);
        assertThat(p.shared().appendedTotal()).isEqualTo(3);
        assertThat(p.shared().rejectedFullCount()).isEqualTo(2);

        // 共享队列仍是最旧的 1,2,3，绝不淘汰更早数据
        assertThat(p.cloudEndpoint().peekBatch(10)).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);

        // 云端照常消费，不被停摆的 archive 拖慢，也只能收到已入队的 1,2,3
        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
        assertThat(p.shared().size()).isEqualTo(3); // 队首由停摆的 archive 决定，未裁剪
        logState("full-archive-down/cloud-drained", p);

        // archive 恢复，消费同样的 1,2,3；队首随后被裁剪
        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertThat(p.shared().size()).isZero();

        // 腾出容量后，之前被拒的 4、5 作为合法新数据重试入队，不被误判为重复，两边最终仍一致
        assertThat(p.fanOut().append(point(4))).isEqualTo(DownstreamFanOut.AppendOutcome.APPENDED);
        assertThat(p.fanOut().append(point(5))).isEqualTo(DownstreamFanOut.AppendOutcome.APPENDED);
        drain(p.topology().get("cloud"));
        drain(p.topology().get("archive"));
        assertThat(p.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertThat(p.cloud().receiver().duplicateCount()).isZero();
        logState("after-retry", p);
    }

    @Test
    void eachDownstreamResumesFromItsOwnProgressAndRepeatedFlushDoesNotResend() {
        // 云端批次=2，归档批次=100，制造两边不同消费进度
        Pair p = topology(100, 2, 100);
        ingest(p.fanOut(), 1, 5);

        p.archive().bringDown();
        assertThat(p.topology().get("cloud").forwarder().flushOnce()).isEqualTo(2);
        assertThat(p.topology().get("archive").forwarder().flushOnce()).isZero();
        assertThat(p.cloudEndpoint().pending()).isEqualTo(3);
        assertThat(p.archiveEndpoint().pending()).isEqualTo(5);
        logState("diverged-progress", p);

        drain(p.topology().get("cloud"));
        assertThat(p.topology().get("cloud").forwarder().flushOnce()).isZero();
        assertThat(p.cloud().received()).hasSize(5);
        assertThat(p.cloud().receiver().duplicateCount()).isZero();

        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).hasSize(5);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        logState("independent-resume", p);
    }
}
