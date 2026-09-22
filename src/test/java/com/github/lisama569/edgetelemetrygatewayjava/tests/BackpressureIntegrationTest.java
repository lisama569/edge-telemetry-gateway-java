package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ArchiveCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService;
import com.github.lisama569.edgetelemetrygatewayjava.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * 背压处置集成测试：
 * 容量上限为 5、两个通道都不可用时，前 5 条进入本地缓存，
 * 第 6 条在 offerTimeoutMs 后得到明确的 BACKPRESSURE_REJECTED 结论
 * （可观测、可重试），而不是被静默丢弃；通道恢复、缓存排空后可重新写入。
 */
@SpringBootTest
class BackpressureIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureIntegrationTest.class);

    @TempDir
    static Path tempDir;
    private static final AtomicInteger CASE = new AtomicInteger();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("edge.gateway.data-dir",
                () -> tempDir.resolve("bp-" + CASE.incrementAndGet()).toString());
        registry.add("edge.gateway.cache-capacity", () -> 5);
        registry.add("edge.gateway.offer-timeout-ms", () -> 300);
        registry.add("edge.gateway.fetch-batch-size", () -> 10);
    }

    @Autowired private IngestService ingest;
    @Autowired private ForwardingService forwarding;
    @Autowired private PendingStore store;
    @Autowired private GatewayMetrics metrics;
    @Autowired private SimulatedCloudChannel primary;
    @Autowired private ArchiveCloudChannel archive;

    @BeforeEach
    void blockChannels() {
        primary.reset();
        archive.reset();
        metrics.reset();
        primary.setAvailable(false);
        archive.setAvailable(false);
        forwarding.kick();
    }

    private String json(long seq) {
        return "{\"sourceId\":\"s1\",\"pointId\":\"p1\",\"seq\":" + seq
                + ",\"timestampMs\":" + (1_727_000_000_000L + seq) + ",\"value\":1.0}";
    }

    @Test
    void cacheFull_explicitBackpressureRejection_thenRecovers() throws Exception {
        for (long seq = 1; seq <= 5; seq++) {
            IngestResult ok = ingest.ingest("json", json(seq));
            log.info("[测试输入] 第 {} 条 -> {}", seq, ok.outcome());
            assertEquals("ACCEPTED", ok.outcome());
        }
        assertEquals(5, store.pendingCount());
        log.info("[关键判定] 缓存已占满 {}/{}", store.pendingCount(), store.capacity());

        long start = System.currentTimeMillis();
        IngestResult rejected = ingest.ingest("json", json(6));
        long elapsed = System.currentTimeMillis() - start;
        log.info("[关键判定] 第 6 条 -> outcome={} reason={} detail={} 耗时={}ms",
                rejected.outcome(), rejected.rejectReason(), rejected.detail(), elapsed);

        assertEquals("BACKPRESSURE_REJECTED", rejected.outcome());
        assertEquals(RejectReason.BACKPRESSURE_REJECTED, rejected.rejectReason());
        assertEquals(1L, metrics.backpressureRejected.get(),
                "背压拒绝必须计入可观测指标");
        assertTrue(elapsed >= 250, "必须经过显式等待，证明非静默丢弃");

        // 通道恢复，缓存排空
        primary.setAvailable(true);
        archive.setAvailable(true);
        forwarding.kick();
        assertTrue(forwarding.awaitDrained("primary", store.lastEntryId(), 10_000));
        assertTrue(forwarding.awaitDrained("archive", store.lastEntryId(), 10_000));

        // 之前被背压拒绝的报文由上游重试，可重新成功写入
        IngestResult retry = ingest.ingest("json", json(6));
        log.info("[关键判定] 恢复后重试第 6 条 -> outcome={} entryId={}", retry.outcome(), retry.entryId());
        assertEquals("ACCEPTED", retry.outcome());
        assertTrue(forwarding.awaitDrained("primary", retry.entryId(), 10_000));
        assertTrue(forwarding.awaitDrained("archive", retry.entryId(), 10_000));
        assertEquals(6L, primary.receivedCount(), "恢复后 6 条全部送达，无丢失");
        assertEquals(6L, archive.receivedCount());
    }
}
