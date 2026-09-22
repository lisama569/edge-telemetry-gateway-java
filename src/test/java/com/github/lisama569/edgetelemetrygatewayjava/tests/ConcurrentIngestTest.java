package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.dedupe.Deduplicator;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ArchiveCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 并发采集测试：多个来源、多线程持续写入，且两通道短暂中断。
 * 可复现结论：
 * - entryId 全局单调、无空洞（所有 ACCEPTED 均落盘）；
 * - 两个云端收到的条目集合与顺序完全一致（按 entryId FIFO）；
 * - accepted == persisted == 两云端各自条数。
 */
@SpringBootTest
class ConcurrentIngestTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestTest.class);

    @TempDir
    static Path tempDir;
    private static final AtomicInteger CASE = new AtomicInteger();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("edge.gateway.data-dir",
                () -> tempDir.resolve("cc-" + CASE.incrementAndGet()).toString());
        registry.add("edge.gateway.cache-capacity", () -> 2000);
        registry.add("edge.gateway.fetch-batch-size", () -> 25);
    }

    @Autowired private IngestService ingest;
    @Autowired private com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService forwarding;
    @Autowired private SimulatedCloudChannel primary;
    @Autowired private ArchiveCloudChannel archive;
    @Autowired private Deduplicator deduplicator;

    @Test
    void concurrentWriters_completeAndOrderedAcrossConsumers() throws Exception {
        deduplicator.reset();
        primary.reset();
        archive.reset();

        int writerThreads = 8;
        int perWriter = 50; // 每线程独立 source，seq 1..50
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Long> acceptedIds = new ConcurrentLinkedQueue<>();
        AtomicInteger rejected = new AtomicInteger();

        // 中途制造一次通道抖动：投递确定性失败一次，触发整批幂等重发
        primary.failNextDeliveries(1);

        log.info("[测试输入] {} 个线程 x {} 条并发写入，主通道注入 1 次失败", writerThreads, perWriter);
        for (int w = 0; w < writerThreads; w++) {
            final String source = "sensor-" + w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int seq = 1; seq <= perWriter; seq++) {
                        String payload = "{\"sourceId\":\"" + source + "\",\"pointId\":\"v\",\"seq\":"
                                + seq + ",\"timestampMs\":" + (1_727_000_000_000L + seq)
                                + ",\"value\":" + seq + "}";
                        IngestResult r = ingest.ingest("json", payload);
                        if ("ACCEPTED".equals(r.outcome())) {
                            acceptedIds.add(r.entryId());
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int total = writerThreads * perWriter;
        log.info("[关键判定] 全部线程完成, 接受={} 拒绝={} 期望={}", acceptedIds.size(), rejected.get(), total);
        assertEquals(0, rejected.get(), "容量充足时不得有背压或其他拒绝");
        assertEquals(total, acceptedIds.size());

        long lastEntryId = acceptedIds.stream().mapToLong(Long::longValue).max().orElseThrow();
        assertTrue(forwarding.awaitDrained("primary", lastEntryId, 20_000));
        assertTrue(forwarding.awaitDrained("archive", lastEntryId, 20_000));

        List<Long> pIds = primary.receivedInOrder().stream().map(StoredEntry::id).toList();
        List<Long> aIds = archive.receivedInOrder().stream().map(StoredEntry::id).toList();
        log.info("[关键判定] primary 条数={} archive 条数={}", pIds.size(), aIds.size());

        // entryId 全局单调、无空洞
        List<Long> sorted = new ArrayList<>(acceptedIds);
        Collections.sort(sorted);
        assertEquals(sorted.get(0) + sorted.size() - 1, sorted.get(sorted.size() - 1),
                "entryId 必须连续无空洞");
        assertEquals(sorted, pIds, "primary 必须按 entryId FIFO 收到全部接受数据");
        assertEquals(pIds, aIds, "两个消费方的顺序与内容必须一致");
        assertEquals(total, primary.receivedCount());
        assertEquals(total, archive.receivedCount());
    }
}
