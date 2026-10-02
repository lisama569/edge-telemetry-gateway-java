package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.SharedFanOutBuffer;
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
 * 双下游强一致性（共享准入日志）对账：
 * - 同一份数据进入 cloud 与 archive，全部 drain 后两边按测点的序号序列逐条一致（条数+顺序）；
 * - 一个下游 down/变慢：只阻塞它自己，不拖另一个；恢复后按自己的游标续传，不重发已确认数据；
 * - 共享缓存写满：整条数据被两侧<strong>一致拒绝</strong>（绝不单侧淘汰最旧造成分叉），
 *   对账如实暴露 admitted/received/buffered/rejectedFull，且两侧序列始终一致（含在途对账）；
 * - 恢复后新来的合法序号正常接收，不被误判成重复。
 */
class DualDownstreamConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamConsistencyTest.class);

    @TempDir
    Path tempDir;

    private record Pair(DownstreamTopology topology,
                        DownstreamFanOut fanOut,
                        SharedFanOutBuffer shared,
                        CloudDownstreamChannel cloud,
                        ArchiveDownstreamChannel archive) {
    }

    private Pair topology(int capacity) {
        return topology(tempDir.resolve("fanout-" + System.nanoTime()), capacity, 100, 100);
    }

    private Pair topology(Path dir, int capacity, int cloudBatch, int archiveBatch) {
        SharedFanOutBuffer shared = SharedFanOutBuffer.create(dir, capacity, List.of("cloud", "archive"));
        TelemetryBuffer cloudView = shared.view("cloud");
        TelemetryBuffer archiveView = shared.view("archive");
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder cloudFwd = new Forwarder("cloud", cloudView, cloud, cloudBatch, 1000);
        Forwarder archiveFwd = new Forwarder("archive", archiveView, archive, archiveBatch, 1000);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudView, cloud, cloudFwd),
                new Downstream("archive", archiveView, archive, archiveFwd)));
        return new Pair(topology, new DownstreamFanOut(shared, shared.names()), shared, cloud, archive);
    }

    private void ingest(DownstreamFanOut fanOut, String point, long from, long to) {
        for (long seq = from; seq <= to; seq++) {
            fanOut.append(new TelemetryPoint("plc-1", point, seq,
                    Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD));
        }
    }

    private void ingest(DownstreamFanOut fanOut, long from, long to) {
        ingest(fanOut, "temperature", from, to);
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

    private void assertBothBalancedAndConsistent(Pair p, int expectedAdmitted) {
        for (Downstream d : p.topology().all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            assertThat(Reconciliation.balanced(d.buffer().appendedCount(), ch.received().size(),
                    d.buffer().size(), d.buffer().droppedCount()))
                    .as("balanced side %s", d.name()).isTrue();
            assertThat(d.buffer().appendedCount()).as("admitted side %s", d.name())
                    .isEqualTo(expectedAdmitted);
        }
        assertThat(Reconciliation.crossDownstreamDiffs(
                p.topology().get("cloud"), p.topology().get("archive")))
                .as("cloud vs archive identical incl. in-flight").isEmpty();
    }

    @Test
    void bothDownstreamsReceiveIdenticalPointSequences() {
        log.info("[场景] 双侧正常：同一批数据两边序号序列一致");
        Pair p = topology(100);
        ingest(p.fanOut(), 1, 10);
        drain(p.topology().get("cloud"));
        drain(p.topology().get("archive"));

        List<Long> cloudSeqs = p.cloud().received().stream().map(TelemetryPoint::seq).toList();
        List<Long> archiveSeqs = p.archive().received().stream().map(TelemetryPoint::seq).toList();
        log.info("cloud seqs={}", cloudSeqs);
        log.info("archive seqs={}", archiveSeqs);

        assertThat(archiveSeqs).containsExactlyElementsOf(cloudSeqs);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertBothBalancedAndConsistent(p, 10);
        logReconciliation(p.topology());
    }

    @Test
    void archiveDownDoesNotBlockCloudAndResumesToIdenticalSequence() {
        log.info("[场景] archive 故障：cloud 照常消费，archive 恢复后续传，最终一致");
        Pair p = topology(100);
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 6);

        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).hasSize(6);
        // archive 一条都没确认，全部在它自己的未确认窗口里
        assertThat(p.topology().get("archive").buffer().size()).isEqualTo(6);
        assertThat(p.archive().received()).isEmpty();
        log.info("archive down: cloudReceived=6 archiveBuffered={}",
                p.topology().get("archive").buffer().size());

        // 即使一边还没恢复，“已收+在途”两侧也必须逐测点一致
        assertThat(Reconciliation.crossDownstreamDiffs(
                p.topology().get("cloud"), p.topology().get("archive"))).isEmpty();

        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertBothBalancedAndConsistent(p, 6);
        logReconciliation(p.topology());
    }

    @Test
    void slowArchiveDoesNotSlowCloudDown() {
        log.info("[场景] archive 变慢：cloud 不被拖慢，drain 后两边一致");
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
        log.info("cloud drained independently of slow archive in {}ms", cloudElapsed);

        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).hasSize(5);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();
        assertBothBalancedAndConsistent(p, 5);
    }

    @Test
    void sharedOverflowRejectsForBothSidesAtomicallyAndNeverDiverges() {
        log.info("[场景] archive 停摆 + 持续写入打满共享缓存：整条两边一致拒绝，单侧不淘汰");
        Pair p = topology(3); // 共享容量仅 3
        p.archive().bringDown();

        // 前 3 条准入；第 4、5 条因为 archive 未确认、共享窗口满，被两侧一致拒绝
        ingest(p.fanOut(), 1, 5);

        TelemetryBuffer cloudView = p.topology().get("cloud").buffer();
        TelemetryBuffer archiveView = p.topology().get("archive").buffer();
        assertThat(p.shared.rejectedCount()).isEqualTo(2);
        assertThat(cloudView.rejectedCount()).isEqualTo(2);
        assertThat(archiveView.rejectedCount()).isEqualTo(2);
        // 双下游路径没有任何单侧淘汰
        assertThat(cloudView.droppedCount()).isZero();
        assertThat(archiveView.droppedCount()).isZero();
        // cloud 即便一直正常，也只能拿到被准入的 1,2,3；4,5 两边都没有
        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
        log.info("shared overflow: admitted=3 rejectedFull=2 cloudSeqs={} archiveBuffered={}",
                p.cloud().received().stream().map(TelemetryPoint::seq).toList(),
                archiveView.size());

        // 恢复 archive：续传 1,2,3，不补 4,5（它们从未被任一下游准入）
        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
        assertThat(Reconciliation.diffByPoint(p.cloud().received(), p.archive().received())).isEmpty();

        // 两侧对账数字真实且平衡：admitted=3 == received 3 + buffered 0 + evicted 0
        for (Downstream d : p.topology().all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            assertThat(ch.received()).hasSize(3);
            assertThat(d.buffer().appendedCount()).isEqualTo(3);
            assertThat(d.buffer().size()).isZero();
            assertThat(d.buffer().droppedCount()).isZero();
            assertThat(Reconciliation.balanced(3, 3, 0, 0)).isTrue();
        }
        log.info("after resume both sides seq=1,2,3; rejectedFull seq=4,5 lost by NEITHER side alone");
        logReconciliation(p.topology());
    }

    @Test
    void resumeAcceptsNewValidSeqsAsNotDuplicatesAndRepeatsDoNotResend() {
        log.info("[场景] 恢复后续传 + 新数据不被误判重复 + 重复 flush 不重发");
        Pair p = topology(100);
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 5);
        drain(p.topology().get("cloud"));

        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(p.archive().receiver().duplicateCount()).isZero();

        // 恢复后继续写入合法新序号 6..8：两边都正常 ACCEPTED，不得误判为重复
        ingest(p.fanOut(), 6, 8);
        drain(p.topology().get("cloud"));
        drain(p.topology().get("archive"));
        assertThat(p.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertThat(p.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertThat(p.cloud().receiver().duplicateCount()).isZero();
        assertThat(p.archive().receiver().duplicateCount()).isZero();

        // 重复触发 flush：已确认数据不再取出/发送
        assertThat(p.topology().get("cloud").forwarder().flushOnce()).isZero();
        assertThat(p.topology().get("archive").forwarder().flushOnce()).isZero();
        assertThat(p.cloud().received()).hasSize(8);
        assertThat(p.archive().received()).hasSize(8);
        assertThat(Reconciliation.crossDownstreamDiffs(
                p.topology().get("cloud"), p.topology().get("archive"))).isEmpty();
        log.info("resume + new seqs accepted, both sides 1..8, no duplicates");
    }

    @Test
    void eachDownstreamResumesFromItsOwnCursorWithDivergedProgress() {
        log.info("[场景] 两边消费进度不同：各自按自己的游标续传，最终逐测点序列一致");
        Path dir = tempDir.resolve("progress");
        Pair p = topology(dir, 100, 2, 100); // cloud 批次 2，archive 批次 100
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 5);

        assertThat(p.topology().get("cloud").forwarder().flushOnce()).isEqualTo(2);
        assertThat(p.topology().get("archive").forwarder().flushOnce()).isZero();
        // 共享日志保留 5 条；cloud 未确认窗口剩 3，archive 仍欠 5
        assertThat(p.topology().get("cloud").buffer().size()).isEqualTo(3);
        assertThat(p.topology().get("archive").buffer().size()).isEqualTo(5);
        log.info("diverged cursors: cloudBuffered=3 archiveBuffered=5");

        drain(p.topology().get("cloud"));
        assertThat(p.cloud().received()).hasSize(5);
        assertThat(p.cloud().receiver().duplicateCount()).isZero();

        p.archive().bringUp();
        drain(p.topology().get("archive"));
        assertThat(p.archive().received()).hasSize(5);
        assertThat(Reconciliation.crossDownstreamDiffs(
                p.topology().get("cloud"), p.topology().get("archive"))).isEmpty();
        logReconciliation(p.topology());
    }

    @Test
    void inFlightReconciliationStaysConsistentWhileOneSideIsBehind() {
        log.info("[场景] 对账如实性：一侧停在半路时 crossDownstreamDiffs 为空、balanced 为真");
        Pair p = topology(100);
        p.archive().bringDown();
        ingest(p.fanOut(), 1, 10);
        // cloud 只消费 4 条，archive 0 条：已收+在途两边仍是同一条共享日志
        int got = 0;
        int n;
        while (got < 4 && (n = p.topology().get("cloud").forwarder().flushOnce()) > 0) {
            got += n;
        }
        Downstream cloud = p.topology().get("cloud");
        Downstream archive = p.topology().get("archive");
        assertThat(Reconciliation.crossDownstreamDiffs(cloud, archive)).isEmpty();
        for (Downstream d : List.of(cloud, archive)) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            assertThat(Reconciliation.balanced(d.buffer().appendedCount(), ch.received().size(),
                    d.buffer().size(), d.buffer().droppedCount())).isTrue();
        }
        log.info("mid-flight snapshot: cloudReceived={} cloudBuffered={} archiveBuffered={}",
                p.cloud().received().size(), cloud.buffer().size(), archive.buffer().size());
        logReconciliation(p.topology());
    }
}
