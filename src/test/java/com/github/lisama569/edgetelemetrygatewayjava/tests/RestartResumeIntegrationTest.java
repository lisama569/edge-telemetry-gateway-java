package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.forward.ArchiveCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import com.github.lisama569.edgetelemetrygatewayjava.Application;

/**
 * 进程重启后续传测试：
 * 第一个运行实例在主通道断连期间写入数据并关闭（模拟崩溃/停机），
 * 第二个实例复用同一数据目录启动，恢复通道后必须把未确认数据继续送达，
 * 且已确认数据不重发、未确认数据不丢失。
 */
class RestartResumeIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RestartResumeIntegrationTest.class);

    @TempDir
    Path tempDir;

    private String json(long seq) {
        return "{\"sourceId\":\"s1\",\"pointId\":\"p1\",\"seq\":" + seq
                + ",\"timestampMs\":" + (1_727_000_000_000L + seq) + ",\"value\":1.0}";
    }

    @Test
    void restart_resumesUnconfirmedAndSkipsConfirmed() throws Exception {
        String dataDir = tempDir.resolve("restart").toString();

        // ---- 第一个实例：归档通道确认前 2 条，主通道始终断开，写入 4 条后停机 ----
        long primaryBefore;
        long archiveBefore;
        try (ConfigurableApplicationContext ctx1 = start(dataDir, false)) {
            SimulatedCloudChannel primary1 = ctx1.getBean(SimulatedCloudChannel.class);
            ArchiveCloudChannel archive1 = ctx1.getBean(ArchiveCloudChannel.class);
            IngestService ingest1 = ctx1.getBean(IngestService.class);
            var forwarding1 = ctx1.getBean(
                    com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService.class);
            var store1 = ctx1.getBean(com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore.class);
            GatewayMetrics metrics1 = ctx1.getBean(GatewayMetrics.class);

            primary1.setAvailable(false);
            forwarding1.kick();
            long failuresSeen = metrics1.channelFailures.get();
            for (long seq = 1; seq <= 4; seq++) {
                IngestResult r = ingest1.ingest("json", json(seq));
                assertEquals("ACCEPTED", r.outcome());
            }
            // 等待主通道 worker 至少观察到一次“不可用”，消除关闭竞态
            long deadline = System.currentTimeMillis() + 5000;
            while (metrics1.channelFailures.get() <= failuresSeen
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            log.info("[实例1] primary worker 已观察到通道不可用 channelFailures={}",
                    metrics1.channelFailures.get());
            assertTrue(metrics1.channelFailures.get() > failuresSeen);
            assertTrue(forwarding1.awaitDrained("archive", store1.lastEntryId(), 10_000));
            log.info("[实例1] 写入 4 条, primary 水位={}, archive 水位={}",
                    store1.watermark("primary"), store1.watermark("archive"));
            primaryBefore = primary1.receivedCount();
            archiveBefore = archive1.receivedCount();
            assertEquals(0L, primaryBefore);
            assertEquals(4L, archiveBefore);
        }
        log.info("[实例1] 已关闭（模拟进程重启）");

        // ---- 第二个实例：先以暂停模式启动，断言恢复状态，再恢复转发续传 ----
        try (ConfigurableApplicationContext ctx2 = start(dataDir, true)) {
            SimulatedCloudChannel primary2 = ctx2.getBean(SimulatedCloudChannel.class);
            ArchiveCloudChannel archive2 = ctx2.getBean(ArchiveCloudChannel.class);
            var store2 = ctx2.getBean(com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore.class);
            var forwarding2 = ctx2.getBean(
                    com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService.class);

            // 暂停期间水位不得有任何推进
            Thread.sleep(500);
            log.info("[实例2] 启动恢复: primary 水位={} archive 水位={} lastEntryId={}",
                    store2.watermark("primary"), store2.watermark("archive"), store2.lastEntryId());
            assertEquals(0L, store2.watermark("primary"));
            assertEquals(4L, store2.watermark("archive"));
            assertEquals(4L, store2.lastEntryId(), "entryId 从 WAL 恢复续号");
            assertEquals(0L, primary2.receivedCount(), "暂停期间不得投递");

            forwarding2.resume();
            assertTrue(forwarding2.awaitDrained("primary", store2.lastEntryId(), 10_000));
            assertTrue(forwarding2.awaitDrained("archive", store2.lastEntryId(), 10_000));

            List<Long> primaryIds = primary2.receivedInOrder().stream().map(StoredEntry::id).toList();
            log.info("[关键判定] 重启后 primary 收到 entryId 序列={}", primaryIds);
            assertEquals(List.of(1L, 2L, 3L, 4L), primaryIds,
                    "未确认的 4 条全部续传，FIFO 顺序，无丢失");
            assertEquals(4L, primary2.receivedCount());
            // archive 重启前已全部确认，重启后不得重发
            log.info("[关键判定] 重启后 archive 新增条数={}（应为 0，已确认不重发）",
                    archive2.receivedCount() - 0);
            assertEquals(0L, archive2.receivedCount(), "已确认数据重启后不重发");
        }
    }

    private ConfigurableApplicationContext start(String dataDir, boolean startPaused) {
        var app = new SpringApplication(Application.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        return app.run(
                "--edge.gateway.data-dir=" + dataDir,
                "--edge.gateway.cache-capacity=100",
                "--edge.gateway.fetch-batch-size=10",
                "--edge.gateway.start-paused=" + startPaused);
    }
}
