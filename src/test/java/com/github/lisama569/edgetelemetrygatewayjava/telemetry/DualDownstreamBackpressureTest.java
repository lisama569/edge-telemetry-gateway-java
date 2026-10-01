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
 * 双下游 + 并发采集 + 慢归档：归档变慢不得拖住云端；
 * 每个下游独立对账 appended == received + buffered + dropped；
 * drain 完两边按测点序列一致。
 */
class DualDownstreamBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamBackpressureTest.class);
    private static final int PRODUCERS = 4;
    private static final int PER_PRODUCER = 200;

    @TempDir
    Path tempDir;

    @Test
    void slowArchiveDoesNotBlockCloudAndBothReconcileUnderConcurrency() throws InterruptedException {
        TelemetryBuffer cloudBuffer = FileTelemetryBuffer.at(tempDir.resolve("cloud.jsonl"), 100000);
        TelemetryBuffer archiveBuffer = FileTelemetryBuffer.at(tempDir.resolve("archive.jsonl"), 100000);
        CloudDownstreamChannel cloud =
                new CloudDownstreamChannel("cloud", new IdempotentReceiver(new InMemoryIdentityStore()));
        ArchiveDownstreamChannel archive =
                new ArchiveDownstreamChannel("archive", new IdempotentReceiver(new InMemoryIdentityStore()));
        archive.setLatencyMillis(2);
        Forwarder cloudFwd = new Forwarder("cloud", cloudBuffer, cloud, 100, 2);
        Forwarder archiveFwd = new Forwarder("archive", archiveBuffer, archive, 100, 2);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudBuffer, cloud, cloudFwd),
                new Downstream("archive", archiveBuffer, archive, archiveFwd)));
        DownstreamFanOut fanOut = new DownstreamFanOut(topology);

        long total = (long) PRODUCERS * PER_PRODUCER;
        CountDownLatch ready = new CountDownLatch(PRODUCERS);
        List<Thread> producers = new ArrayList<>();
        for (int p = 0; p < PRODUCERS; p++) {
            String pointId = "point-" + p;
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    ready.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (long seq = 1; seq <= PER_PRODUCER; seq++) {
                    fanOut.append(new TelemetryPoint("plc-" + pointId, pointId, seq,
                            Instant.ofEpochMilli(1758000000000L), seq, TelemetryPoint.Quality.GOOD));
                }
            }, "producer-" + p);
            producers.add(t);
            t.start();
        }
        cloudFwd.start();
        archiveFwd.start();
        for (Thread t : producers) {
            t.join();
        }

        // 等待两边各自 drain 完成
        await().atMost(Duration.ofSeconds(40)).until(() ->
                cloud.received().size() == total && archive.received().size() == total);
        cloudFwd.stop();
        archiveFwd.stop();

        for (Downstream d : topology.all()) {
            long received = ((com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream
                    .SimulatedDownstreamChannel) d.channel()).received().size();
            log.info("concurrent reconciliation[{}]: received={} buffered={} dropped={}",
                    d.name(), received, d.buffer().size(), d.buffer().droppedCount());
            assertThat(Reconciliation.balanced(total, received, d.buffer().size(), d.buffer().droppedCount()))
                    .isTrue();
            assertThat(d.buffer().size()).isZero();
            assertThat(d.buffer().droppedCount()).isZero();
        }

        // 两边条数一致且按测点序列逐条相同
        assertThat(cloud.received()).hasSize((int) total);
        assertThat(archive.received()).hasSize((int) total);
        assertThat(Reconciliation.diffByPoint(cloud.received(), archive.received())).isEmpty();
        log.info("cloud vs archive identical under concurrency, total identities per side={}", total);
    }
}
