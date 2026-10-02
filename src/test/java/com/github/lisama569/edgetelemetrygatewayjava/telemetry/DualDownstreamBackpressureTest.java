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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 双下游 + 并发采集下的最终一致性：
 * - 慢归档不阻塞云端；各自独立游标，全部 drain 后两边逐测点序号序列完全一致；
 * - 归档停摆且共享缓存被并发写满时，整条数据两侧一致拒绝（单侧不淘汰），
 *   恢复归档续传后，被准入的数据两边逐条一致，且新合法数据不被误判重复；
 * - 并发下共享准入日志串行化 append：每个测点的全局顺序两边相同、无重复。
 */
class DualDownstreamBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamBackpressureTest.class);
    private static final int PRODUCERS = 4;
    private static final int PER_PRODUCER = 200;

    @TempDir
    Path tempDir;

    private record Rig(SharedFanOutBuffer shared,
                       DownstreamTopology topology,
                       DownstreamFanOut fanOut,
                       CloudDownstreamChannel cloud,
                       ArchiveDownstreamChannel archive,
                       Forwarder cloudFwd,
                       Forwarder archiveFwd) {
    }

    private Rig rig(String dirName, int capacity, long latency, int intervalMillis) {
        SharedFanOutBuffer shared = SharedFanOutBuffer.create(
                tempDir.resolve(dirName), capacity, List.of("cloud", "archive"));
        TelemetryBuffer cloudView = shared.view("cloud");
        TelemetryBuffer archiveView = shared.view("archive");
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        archive.setLatencyMillis(latency);
        Forwarder cloudFwd = new Forwarder("cloud", cloudView, cloud, 100, intervalMillis);
        Forwarder archiveFwd = new Forwarder("archive", archiveView, archive, 100, intervalMillis);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudView, cloud, cloudFwd),
                new Downstream("archive", archiveView, archive, archiveFwd)));
        return new Rig(shared, topology, new DownstreamFanOut(shared, shared.names()),
                cloud, archive, cloudFwd, archiveFwd);
    }

    private Thread[] startProducers(Rig rig, int producers, int perProducer, CountDownLatch ready) {
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            String pointId = "point-" + p;
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    ready.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (long seq = 1; seq <= perProducer; seq++) {
                    rig.fanOut().append(new TelemetryPoint("plc-" + pointId, pointId, seq,
                            Instant.ofEpochMilli(1758000000000L), seq, TelemetryPoint.Quality.GOOD));
                }
            }, "producer-" + p);
            threads.add(t);
            t.start();
        }
        return threads.toArray(Thread[]::new);
    }

    @Test
    void slowArchiveDoesNotBlockCloudAndBothConvergeUnderConcurrency() throws InterruptedException {
        log.info("[场景] 并发 {} 生产者 x {} 条 + archive 慢：cloud 不被拖慢，最终两边一致",
                PRODUCERS, PER_PRODUCER);
        Rig rig = rig("slow", 100_000, 2, 2);
        long total = (long) PRODUCERS * PER_PRODUCER;

        CountDownLatch ready = new CountDownLatch(PRODUCERS);
        Thread[] producers = startProducers(rig, PRODUCERS, PER_PRODUCER, ready);
        rig.cloudFwd().start();
        rig.archiveFwd().start();
        for (Thread t : producers) {
            t.join();
        }

        await().atMost(Duration.ofSeconds(40)).until(() ->
                rig.cloud().received().size() == total && rig.archive().received().size() == total);
        rig.cloudFwd().stop();
        rig.archiveFwd().stop();

        for (Downstream d : rig.topology().all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            log.info("concurrent reconciliation[{}]: received={} buffered={} evicted={} rejectedFull={}",
                    d.name(), ch.received().size(), d.buffer().size(),
                    d.buffer().droppedCount(), d.buffer().rejectedCount());
            assertThat(Reconciliation.balanced(d.buffer().appendedCount(), ch.received().size(),
                    d.buffer().size(), d.buffer().droppedCount())).isTrue();
            assertThat(d.buffer().size()).isZero();
            assertThat(d.buffer().droppedCount()).isZero();
            assertThat(d.buffer().rejectedCount()).isZero();
        }
        assertThat(Reconciliation.diffByPoint(rig.cloud().received(), rig.archive().received())).isEmpty();
        log.info("cloud vs archive identical under concurrency, total per side={}", total);
    }

    @Test
    void archiveDownWithConcurrentWritersFillingSharedBufferConvergesAfterRecovery()
            throws InterruptedException {
        log.info("[场景] archive 故障 + 并发写入打满共享缓存：整条一致拒绝，恢复续传后两边一致");
        int capacity = 300;
        Rig rig = rig("overflow", capacity, 0, 2);
        rig.archive().bringDown();

        // 只写 200/测点：总量 800 > 容量 300，前 300 准入，其余两侧一致拒绝
        CountDownLatch ready = new CountDownLatch(PRODUCERS);
        Thread[] producers = startProducers(rig, PRODUCERS, PER_PRODUCER, ready);
        rig.cloudFwd().start();
        // archive 转发器保持停止（通道 down，flush 会跳过），制造“最慢一侧完全不确认”
        for (Thread t : producers) {
            t.join();
        }

        // 等待 cloud 把已准入的 300 条全部消费完；archive 仍 0
        await().atMost(Duration.ofSeconds(20)).until(() ->
                rig.cloud().received().size() == capacity
                        && rig.topology().get("cloud").buffer().size() == 0);
        long rejectedWhileDown = rig.shared().rejectedCount();
        assertThat(rejectedWhileDown)
                .as("overflow rejections happen and are visible")
                .isEqualTo((long) PRODUCERS * PER_PRODUCER - capacity);
        assertThat(rig.archive().received()).isEmpty();
        log.info("while archive down: cloud received={} shared rejectedFull={} (both sides lack them)",
                rig.cloud().received().size(), rejectedWhileDown);

        // 在 cloud 已 drain 的情况下对账：archive 全在途，两边“已收+在途”仍必须一致
        assertThat(Reconciliation.crossDownstreamDiffs(
                rig.topology().get("cloud"), rig.topology().get("archive"))).isEmpty();

        // archive 恢复，按自己的游标续传 300 条；重复身份幂等，序列逐条相同
        rig.archive().bringUp();
        rig.archiveFwd().start();
        await().atMost(Duration.ofSeconds(20)).until(() ->
                rig.archive().received().size() == capacity);
        rig.cloudFwd().stop();
        rig.archiveFwd().stop();

        assertThat(Reconciliation.diffByPoint(rig.cloud().received(), rig.archive().received())).isEmpty();
        assertThat(rig.archive().receiver().duplicateCount()).isZero();
        for (Downstream d : rig.topology().all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            assertThat(ch.received()).hasSize(capacity);
            assertThat(d.buffer().appendedCount()).isEqualTo(capacity);
            assertThat(d.buffer().size()).isZero();
            assertThat(d.buffer().droppedCount()).isZero(); // 从未单侧淘汰
            assertThat(d.buffer().rejectedCount()).isEqualTo(rejectedWhileDown);
            assertThat(Reconciliation.balanced(capacity, capacity, 0, 0)).isTrue();
        }

        // 容量腾出后，新来的合法数据（seq=201..210）两边都正常收，不被误判重复
        for (int p = 0; p < PRODUCERS; p++) {
            String pointId = "point-" + p;
            for (long seq = PER_PRODUCER + 1; seq <= PER_PRODUCER + 10; seq++) {
                TelemetryBuffer.AppendOutcome outcome = rig.fanOut().append(
                        new TelemetryPoint("plc-" + pointId, pointId, seq,
                                Instant.ofEpochMilli(1758000000000L), seq, TelemetryPoint.Quality.GOOD));
                assertThat(outcome.status()).isEqualTo(TelemetryBuffer.AppendStatus.APPENDED);
            }
        }
        drainAll(rig);
        assertThat(rig.cloud().received()).hasSize(capacity + PRODUCERS * 10);
        assertThat(rig.archive().received()).hasSize(capacity + PRODUCERS * 10);
        assertThat(rig.cloud().receiver().duplicateCount()).isZero();
        assertThat(rig.archive().receiver().duplicateCount()).isZero();
        assertThat(Reconciliation.crossDownstreamDiffs(
                rig.topology().get("cloud"), rig.topology().get("archive"))).isEmpty();
        log.info("after recovery + new seqs: both sides identical, size={}, rejectedFull remained={}",
                rig.cloud().received().size(), rejectedWhileDown);
    }

    private void drainAll(Rig rig) {
        // 两个转发器已 stop，这里手动同步 drain，确保结论确定可复现
        AtomicLong any = new AtomicLong(1);
        while (any.get() > 0) {
            any.set(0);
            any.addAndGet(rig.topology().get("cloud").forwarder().flushOnce());
            any.addAndGet(rig.topology().get("archive").forwarder().flushOnce());
        }
    }
}
