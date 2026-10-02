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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 共享扇出 + 并发采集 + 慢/停下游 + 缓存写满背压 的端到端最终一致性：
 * - 一个下游慢/停时，正常一侧持续独立消费，不被拖慢、不多收不漏收；
 * - 共享队列写满后写入被确定性拒绝（背压），绝不淘汰；慢侧恢复后被拒数据重试补齐；
 * - 全部 drain 后两边按测点的序号序列完全一致，无重复、无永久缺口。
 */
class DualDownstreamBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamBackpressureTest.class);
    private static final int PRODUCERS = 4;
    private static final int PER_PRODUCER = 200;
    private static final long TOTAL = (long) PRODUCERS * PER_PRODUCER;

    @TempDir
    Path tempDir;

    private record Fixture(FileSharedFanOutBuffer shared,
                           BufferEndpoint cloudEndpoint,
                           BufferEndpoint archiveEndpoint,
                           CloudDownstreamChannel cloud,
                           ArchiveDownstreamChannel archive,
                           Forwarder cloudFwd,
                           Forwarder archiveFwd,
                           DownstreamTopology topology,
                           DownstreamFanOut fanOut) {
    }

    private Fixture fixture(String dir, int capacity) {
        FileSharedFanOutBuffer shared = new FileSharedFanOutBuffer(tempDir.resolve(dir), capacity);
        BufferEndpoint cloudEndpoint = shared.subscribe("cloud");
        BufferEndpoint archiveEndpoint = shared.subscribe("archive");
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        Forwarder cloudFwd = new Forwarder("cloud", cloudEndpoint, cloud, 100, 2);
        Forwarder archiveFwd = new Forwarder("archive", archiveEndpoint, archive, 100, 2);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                Downstream.shared("cloud", cloud, cloudFwd, cloudEndpoint),
                Downstream.shared("archive", archive, archiveFwd, archiveEndpoint)), shared);
        return new Fixture(shared, cloudEndpoint, archiveEndpoint, cloud, archive,
                cloudFwd, archiveFwd, topology, new DownstreamFanOut(shared));
    }

    private TelemetryPoint point(int p, long seq) {
        String pointId = "point-" + p;
        return new TelemetryPoint("plc-" + pointId, pointId, seq,
                Instant.ofEpochMilli(1758000000000L), seq, TelemetryPoint.Quality.GOOD);
    }

    /** 并发投递所有测点的 1..PER_PRODUCER；满则退避自旋重试直到入队（用于下游在消费的场景）。 */
    private void produceAllWithRetry(Fixture f) throws InterruptedException {
        produceRange(f, 1, true);
    }

    /** 从指定轮次开始并发投递到 PER_PRODUCER。 */
    /**
     * 并发投递每个测点的 fromRound..PER_PRODUCER；按“轮”栅栏推进：每轮 4 个测点各写一条，
     * 因此入队的是完整轮次——每个测点都恰好拥有相同序号集合，便于确定性推导恢复后的补齐起点。
     * 满时 retry=true 退避重试直到整轮入队；retry=false 时遇到写满即停止后续所有轮次（背压跳过）。
     */
    private void produceRange(Fixture f, long fromRound, boolean retry) throws InterruptedException {
        for (long seq = fromRound; seq <= PER_PRODUCER; seq++) {
            final long current = seq;
            DownstreamFanOut.AppendOutcome[] outcomes =
                    new DownstreamFanOut.AppendOutcome[PRODUCERS];
            CountDownLatch done = new CountDownLatch(PRODUCERS);
            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < PRODUCERS; p++) {
                final int producer = p;
                Thread t = new Thread(() -> {
                    try {
                        DownstreamFanOut.AppendOutcome outcome;
                        do {
                            outcome = f.fanOut().append(point(producer, current));
                            if (outcome == DownstreamFanOut.AppendOutcome.REJECTED_FULL && !retry) {
                                break;
                            }
                            if (outcome == DownstreamFanOut.AppendOutcome.REJECTED_FULL) {
                                try {
                                    Thread.sleep(1); // 退避，等待下游消费释放容量
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        } while (outcome == DownstreamFanOut.AppendOutcome.REJECTED_FULL);
                        outcomes[producer] = outcome;
                    } finally {
                        done.countDown();
                    }
                }, "producer-" + p + "-seq" + seq);
                threads.add(t);
                t.start();
            }
            done.await();
            boolean anyFull = false;
            for (DownstreamFanOut.AppendOutcome o : outcomes) {
                anyFull |= o == DownstreamFanOut.AppendOutcome.REJECTED_FULL;
            }
            if (!retry && anyFull) {
                // 该轮写满：未入队的本轮与后续轮次留待恢复后补齐
                break;
            }
        }
    }

    private void assertBothContainExactlyOneThroughN(Fixture f) {
        for (int p = 0; p < PRODUCERS; p++) {
            String pointId = "point-" + p;
            List<Long> expected = new ArrayList<>();
            for (long s = 1; s <= PER_PRODUCER; s++) {
                expected.add(s);
            }
            List<Long> cloudSeqs = f.cloud().received().stream()
                    .filter(x -> x.pointId().equals(pointId)).map(TelemetryPoint::seq)
                    .sorted().toList();
            List<Long> archiveSeqs = f.archive().received().stream()
                    .filter(x -> x.pointId().equals(pointId)).map(TelemetryPoint::seq)
                    .sorted().toList();
            assertThat(cloudSeqs).as("cloud seqs for %s", pointId).isEqualTo(expected);
            assertThat(archiveSeqs).as("archive seqs for %s", pointId).isEqualTo(expected);
        }
    }

    @Test
    void slowArchiveDoesNotBlockCloudAndBothReconcileUnderConcurrency() throws InterruptedException {
        Fixture f = fixture("fanout", 100000);
        f.archive().setLatencyMillis(2);

        produceAllWithRetry(f);
        f.cloudFwd().start();
        f.archiveFwd().start();

        await().atMost(Duration.ofSeconds(40)).until(() ->
                f.cloud().received().size() == TOTAL && f.archive().received().size() == TOTAL);
        f.cloudFwd().stop();
        f.archiveFwd().stop();

        for (Downstream d : f.topology().all()) {
            long received = ((com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream
                    .SimulatedDownstreamChannel) d.channel()).received().size();
            log.info("concurrent reconciliation[{}]: received={} pending={}",
                    d.name(), received, d.endpoint().pending());
            assertThat(d.endpoint().pending()).isZero();
            assertThat(received).isEqualTo(TOTAL);
        }
        assertThat(f.shared().size()).isZero();
        assertThat(f.shared().appendedTotal()).isEqualTo(TOTAL);
        assertThat(f.shared().rejectedFullCount()).isZero();
        assertThat(Reconciliation.diffByPoint(f.cloud().received(), f.archive().received())).isEmpty();
        log.info("cloud vs archive identical under concurrency, total identities per side={}", TOTAL);
    }

    @Test
    void stoppedArchiveFillsSharedBufferThenRejectedWritesAreRetriedAfterRecovery() throws InterruptedException {
        int capacity = 300; // 远小于总量 800，制造写满背压
        Fixture f = fixture("fanout-full", capacity);

        // 阶段一：archive 全程停摆，云端后台消费；生产者尽力投递，满即跳过，不重试。
        f.archive().bringDown();
        f.cloudFwd().start();
        produceRange(f, 1, false);
        // 等云端把已入队的数据全部消费完
        await().atMost(Duration.ofSeconds(20)).until(() ->
                f.cloudEndpoint().pending() == 0 && f.cloud().received().size() == f.shared().appendedTotal());
        f.cloudFwd().stop();

        long appendedPhase1 = f.shared().appendedTotal();
        log.info("phase1 archive down: appended={} (capacity={}) rejectedAttempts={} cloudReceived={} sharedPending={}",
                appendedPhase1, capacity, f.shared().rejectedFullCount(),
                f.cloud().received().size(), f.shared().size());

        // 确定性断言：不超容量；archive 0 确认 -> 队首无法裁剪，队列满载；无任何淘汰。
        assertThat(appendedPhase1).isLessThanOrEqualTo(capacity);
        assertThat(appendedPhase1).isGreaterThan(0);
        assertThat(f.shared().size()).isEqualTo(appendedPhase1);
        assertThat(f.archive().received()).isEmpty();
        assertThat(f.cloud().received()).hasSize((int) appendedPhase1);
        assertThat(f.shared().rejectedFullCount()).isGreaterThan(0);

        // 阶段二：archive 恢复，两边 drain 空，已入队部分先达成一致（云端无重复）。
        f.archive().bringUp();
        f.cloudFwd().start();
        f.archiveFwd().start();
        await().atMost(Duration.ofSeconds(20)).until(() -> f.shared().size() == 0);
        f.cloudFwd().stop();
        f.archiveFwd().stop();
        assertThat(f.archive().received()).hasSize((int) appendedPhase1);
        assertThat(Reconciliation.diffByPoint(f.cloud().received(), f.archive().received())).isEmpty();
        assertThat(f.cloud().receiver().duplicateCount()).isZero();

        // 阶段三：把因写满未入队的轮次（每测点 appendedPhase1/PRODUCERS+1 .. PER_PRODUCER）
        // 作为合法新数据重试补齐。每轮 4 点、入队的是完整轮次，因此补齐起点可由总入队数整除推出。
        // 先启动两个转发器，容量会随消费持续释放，生产者的满队列重试才能推进。
        long completedRounds = appendedPhase1 / PRODUCERS;
        f.cloudFwd().start();
        f.archiveFwd().start();
        produceRange(f, completedRounds + 1, true);
        await().atMost(Duration.ofSeconds(30)).until(() ->
                f.cloud().received().size() == TOTAL && f.archive().received().size() == TOTAL);
        f.cloudFwd().stop();
        f.archiveFwd().stop();

        assertThat(f.shared().appendedTotal()).isEqualTo(TOTAL);
        assertThat(f.shared().size()).isZero();
        assertBothContainExactlyOneThroughN(f);
        assertThat(Reconciliation.diffByPoint(f.cloud().received(), f.archive().received())).isEmpty();
        assertThat(f.cloud().receiver().duplicateCount()).isZero();
        assertThat(f.archive().receiver().duplicateCount()).isZero();
        log.info("final consistency restored after backpressure+recovery: per side={}, rejectedFullAttempts={}",
                TOTAL, f.shared().rejectedFullCount());
    }
}
