package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ArchiveCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.CloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService;
import com.github.lisama569.edgetelemetrygatewayjava.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.dedupe.Deduplicator;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.nio.file.Path;
import java.util.List;
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
 * 端到端集成测试（Spring 容器，真实 WAL + 真实转发 worker）：
 * 1. 乱序/重复/迟到/非法报文的处置结论与拒绝原因可区分；
 * 2. 主通道中断期间本地缓存、恢复后续传，云端不重不丢；
 * 3. 两个消费方收到的数据内容与顺序一致（按 entryId FIFO）。
 */
@SpringBootTest
class GatewayEndToEndTest {

    @TempDir
    static Path tempDir;

    private static final java.util.concurrent.atomic.AtomicInteger CASE =
            new java.util.concurrent.atomic.AtomicInteger();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("edge.gateway.data-dir",
                () -> tempDir.resolve("e2e-" + CASE.incrementAndGet()).toString());
        registry.add("edge.gateway.cache-capacity", () -> 100);
        registry.add("edge.gateway.fetch-batch-size", () -> 10);
    }

    private static final Logger log = LoggerFactory.getLogger(GatewayEndToEndTest.class);

    @Autowired private IngestService ingest;
    @Autowired private ForwardingService forwarding;
    @Autowired private PendingStore store;
    @Autowired private GatewayMetrics metrics;
    @Autowired private SimulatedCloudChannel primary;
    @Autowired private ArchiveCloudChannel archive;
    @Autowired private Deduplicator deduplicator;

    @BeforeEach
    void reset() {
        metrics.reset();
        primary.reset();
        archive.reset();
        primary.setAvailable(true);
        archive.setAvailable(true);
        deduplicator.reset();
    }

    private String json(long seq, double value) {
        return "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":" + seq
                + ",\"timestampMs\":" + (1_727_000_000_000L + seq) + ",\"value\":" + value + "}";
    }

    @Test
    void outOfOrderDuplicateAndInvalid_classifiedDistinctly() throws Exception {
        log.info("[测试输入] 乱序/重复/非法报文序列开始");

        IngestResult r2 = ingest.ingest("json", json(2, 23.4));
        IngestResult r1Late = ingest.ingest("json", json(1, 23.1));
        IngestResult r2Dup = ingest.ingest("json", json(2, 99.9));
        IngestResult r3 = ingest.ingest("json", json(3, 23.9));
        IngestResult bad = ingest.ingest("json", "not-json");
        IngestResult missing = ingest.ingest("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":4}");
        IngestResult unknown = ingest.ingest("modbus", "xx");

        log.info("[关键判定] r2={}, r1Late={}, r2Dup={}, r3={}", r2.outcome(), r1Late.outcome(),
                r2Dup.outcome(), r3.outcome());
        log.info("[关键判定] bad={}/{}, missing={}/{}, unknown={}/{}",
                bad.outcome(), bad.rejectReason(), missing.outcome(), missing.rejectReason(),
                unknown.outcome(), unknown.rejectReason());

        assertEquals("ACCEPTED", r2.outcome());
        assertEquals("STALE", r1Late.outcome());
        assertEquals(RejectReason.STALE, r1Late.rejectReason());
        assertEquals("DUPLICATE", r2Dup.outcome());
        assertEquals(RejectReason.DUPLICATE, r2Dup.rejectReason());
        assertEquals("ACCEPTED", r3.outcome());
        assertEquals("REJECTED", bad.outcome());
        assertEquals(RejectReason.MALFORMED, bad.rejectReason());
        assertEquals(RejectReason.MISSING_FIELD, missing.rejectReason());
        assertEquals(RejectReason.UNKNOWN_PROTOCOL, unknown.rejectReason());

        // 两条被接受的读数都应送达两个云端
        long lastId = r3.entryId();
        assertTrue(forwarding.awaitDrained("primary", lastId, 10_000));
        assertTrue(forwarding.awaitDrained("archive", lastId, 10_000));
        log.info("[关键判定] 两个消费方水位均推进至 {}", lastId);

        // 云端收到的顺序必须按 entryId 升序
        List<Long> primaryIds = primary.receivedInOrder().stream().map(StoredEntry::id).toList();
        List<Long> archiveIds = archive.receivedInOrder().stream().map(StoredEntry::id).toList();
        log.info("[关键判定] primary 收到 entryId 序列={}", primaryIds);
        log.info("[关键判定] archive 收到 entryId 序列={}", archiveIds);
        assertEquals(List.of(r2.entryId(), r3.entryId()), primaryIds);
        assertEquals(primaryIds, archiveIds, "两个消费方顺序与内容必须一致");
        assertEquals(2L, primary.receivedCount());
    }

    @Test
    void channelOutage_buffersLocallyAndResumesWithoutDuplicateOrLoss() throws Exception {
        // 1) 先让一条正常数据通过，建立稳定基线
        IngestResult base = ingest.ingest("json", json(1, 20.1));
        assertTrue(forwarding.awaitDrained("primary", base.entryId(), 10_000));
        long before = primary.receivedCount();

        // 2) 主通道断开；归档通道保持可用
        primary.setAvailable(false);
        forwarding.kick();

        // 3) 断连期间继续采集 5 条
        for (long seq = 2; seq <= 6; seq++) {
            IngestResult r = ingest.ingest("json", json(seq, 20.0 + seq));
            assertEquals("ACCEPTED", r.outcome(), "断连期间数据应进入本地缓存而非丢弃");
        }
        log.info("[测试输入] 主通道断连期间写入 5 条, primary 水位={} pending={}",
                store.watermark("primary"), store.pendingCount());
        assertFalse(forwarding.awaitDrained("primary", store.lastEntryId(), 800),
                "通道不可用时不应推进水位");
        long deliveredWhileDown = primary.receivedCount();
        assertEquals(before, deliveredWhileDown, "断连期间主云端不应收到数据");

        // 归档通道在断连期间持续转发
        assertTrue(forwarding.awaitDrained("archive", store.lastEntryId(), 10_000));
        log.info("[关键判定] 归档通道在主通道断连期间已追平到 {}", store.lastEntryId());

        // 4) 主通道恢复，等待续传完成
        primary.setAvailable(true);
        forwarding.kick();
        assertTrue(forwarding.awaitDrained("primary", store.lastEntryId(), 10_000),
                "恢复后必须从本地缓存续传至最新");
        log.info("[关键判定] 主通道恢复后水位={}, 云端累计条数={}",
                store.watermark("primary"), primary.receivedCount());

        // 5) 不丢：基线 + 5 条全部到达；不重：总数恰好
        assertEquals(before + 5, primary.receivedCount());

        // 6) 顺序：云端记录按 entryId 升序，与归档通道内容一致
        List<Long> primaryIds = primary.receivedInOrder().stream().map(StoredEntry::id).toList();
        List<Long> archiveIds = archive.receivedInOrder().stream().map(StoredEntry::id).toList();
        log.info("[关键判定] primary 序列={}", primaryIds);
        assertEquals(archiveIds, primaryIds, "恢复续传后两消费方数据一致");
        long expectedFirst = base.entryId();
        for (int i = 0; i < primaryIds.size(); i++) {
            assertEquals(expectedFirst + i, primaryIds.get(i), "entryId 必须严格 FIFO 递增");
        }
    }

    @Test
    void transientDeliveryFailure_retriesWithIdempotentKey_noDuplicate() throws Exception {
        // 让主通道接下来 2 次投递确定性失败，第 3 次成功
        primary.failNextDeliveries(2);
        IngestResult r = ingest.ingest("json", json(1, 42.0));
        assertTrue(forwarding.awaitDrained("primary", r.entryId(), 10_000));
        log.info("[关键判定] 两次脚本化失败后投递成功, retryAttempts={}, 云端条数={}",
                metrics.retryAttempts.get(), primary.receivedCount());
        assertTrue(metrics.retryAttempts.get() >= 2);
        assertEquals(1L, primary.receivedCount(), "整批重发由云端幂等去重，不得重复计数");
        assertEquals(1L, archive.receivedCount());
    }
}
