package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DedupGate;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.KeyValueNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 并发采集 + 慢速通道下的背压测试：
 * - 采集侧持续写入不得静默丢弃（对账不变式：accepted == received + buffered + dropped）；
 * - 单消费循环保证每个测点的转发顺序可复现（seq 单调不减）。
 */
class BackpressureConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureConcurrencyTest.class);
    private static final int PRODUCERS = 4;
    private static final int PER_PRODUCER = 250;

    @TempDir
    Path bufferDir;

    @Test
    void concurrentIngestWithSlowChannelLosesNothingSilently() throws InterruptedException {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 500);
        SimulatedCloudChannel channel = new SimulatedCloudChannel();
        channel.setLatencyMillis(2); // 模拟处理缓慢的云端
        Forwarder forwarder = new Forwarder(buffer, channel, 50, 5);
        IngestService ingestService = new IngestService(
                List.of(new KeyValueNormalizer()), new DedupGate(), buffer);

        ConcurrentLinkedQueue<IngestResult> results = new ConcurrentLinkedQueue<>();
        CountDownLatch ready = new CountDownLatch(PRODUCERS);
        List<Thread> threads = new ArrayList<>();
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
                    String payload = "source=plc-" + pointId + ";point=" + pointId + ";seq=" + seq
                            + ";ts=1758000000000;value=" + seq;
                    results.add(ingestService.ingest(new RawFrame("kv", payload)));
                }
            }, "producer-" + p);
            threads.add(t);
            t.start();
        }
        forwarder.start();
        for (Thread t : threads) {
            t.join();
        }

        long expected = (long) PRODUCERS * PER_PRODUCER;
        // 等待 drain 完成（或缓存稳定在溢出平衡态）
        await().atMost(Duration.ofSeconds(30)).until(() ->
                channel.received().size() + buffer.size() + buffer.droppedCount() >= expected);
        forwarder.stop();

        long accepted = results.stream().filter(r -> r instanceof IngestResult.Accepted).count();
        long received = channel.received().size();
        long buffered = buffer.size();
        long dropped = buffer.droppedCount();
        log.info("backpressure reconciliation: accepted={} received={} buffered={} dropped={} "
                        + "flushRounds={} failedRounds={}",
                accepted, received, buffered, dropped,
                forwarder.flushRounds(), forwarder.failedRounds());

        // 对账不变式：无静默丢弃
        assertThat(accepted).isEqualTo(expected);
        assertThat(received + buffered + dropped).isEqualTo(accepted);

        // 顺序可复现：每个测点已转发 seq 单调递增且无重复
        Map<String, List<Long>> seqsByPoint = new TreeMap<>();
        for (TelemetryPoint tp : channel.received()) {
            seqsByPoint.computeIfAbsent(tp.key(), k -> new ArrayList<>()).add(tp.seq());
        }
        seqsByPoint.forEach((key, seqs) -> {
            log.info("point={} received={} first={} last={}", key, seqs.size(),
                    seqs.getFirst(), seqs.getLast());
            assertThat(seqs).isSorted().doesNotHaveDuplicates();
        });
    }
}
