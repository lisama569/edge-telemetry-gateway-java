package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.config.GatewayProperties;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存语义测试：
 * - 多消费方各自水位，只有全部确认后才压缩清除；
 * - confirm 后 fetch 不重复取到（不重复），水位之后的条目始终可取到（不丢失）；
 * - 模拟重启后从 WAL + 水位恢复，续号单调、续传结果一致。
 */
class PendingStoreTest {

    private static final Logger log = LoggerFactory.getLogger(PendingStoreTest.class);

    private GatewayProperties props(Path dir, int capacity) {
        GatewayProperties p = new GatewayProperties();
        p.setDataDir(dir.toString());
        p.setCacheCapacity(capacity);
        p.setFsync(true);
        return p;
    }

    private Reading reading(long seq) {
        return new Reading("s1", "temp", seq, 1_727_000_000_000L + seq, 20.0 + seq);
    }

    @Test
    void twoConsumers_independentWatermarksAndCompaction() throws Exception {
        Path dir = Path.of("target/test-data/store-two-consumers");
        deleteRecursively(dir);
        PendingStore store = new PendingStore(props(dir, 100));
        store.recover();
        store.loadWatermarks(List.of("primary", "archive"));

        for (long seq = 1; seq <= 5; seq++) {
            long id = store.append(reading(seq), 1000);
            assertTrue(id > 0);
        }
        log.info("[测试输入] 追加 5 条, pending={}", store.pendingCount());
        assertEquals(5, store.pendingCount());

        // primary 确认前 3 条；archive 只确认第 1 条。
        store.confirm("primary", store.fetch("primary", 100).get(2).id());
        store.confirm("archive", store.fetch("archive", 100).get(0).id());
        // 最慢消费方 archive 水位为 1：entryId>1 的 4 条仍需转发，占用容量；
        // entryId=1 已被全部确认，可从 WAL/内存物理清除。
        log.info("[关键判定] primary 水位=3, archive 水位=1, pending={}（最慢消费方决定容量占用）",
                store.pendingCount());
        assertEquals(4, store.pendingCount());

        // archive 追平后 entryId<=3 才允许清除
        store.confirm("archive", 3);
        log.info("[关键判定] archive 追平到 3 后 pending={}（entryId 4,5 仍待转发）", store.pendingCount());
        assertEquals(2, store.pendingCount());

        // 已确认的不会被再次 fetch 到（不重复）
        List<StoredEntry> primaryNext = store.fetch("primary", 100);
        assertEquals(List.of(4L, 5L), primaryNext.stream().map(StoredEntry::id).toList());
        List<StoredEntry> archiveNext = store.fetch("archive", 100);
        assertEquals(List.of(4L, 5L), archiveNext.stream().map(StoredEntry::id).toList());
    }

    @Test
    void restart_resumesFromWalAndWatermarks_withoutDuplicateOrLoss() throws Exception {
        Path dir = Path.of("target/test-data/store-restart");
        deleteRecursively(dir);

        // 第一轮：primary 已确认到 2，archive 已确认到 1，随后“重启”
        PendingStore first = new PendingStore(props(dir, 100));
        first.recover();
        first.loadWatermarks(List.of("primary", "archive"));
        long id1 = first.append(reading(1), 1000);
        long id2 = first.append(reading(2), 1000);
        long id3 = first.append(reading(3), 1000);
        first.confirm("primary", id2);
        first.confirm("archive", id1);
        log.info("[测试输入] 重启前: entryId 1..3, primary 水位=2, archive 水位=1");

        // 第二轮：新实例从磁盘恢复
        PendingStore second = new PendingStore(props(dir, 100));
        second.recover();
        second.loadWatermarks(List.of("primary", "archive"));
        log.info("[关键判定] 恢复后 primary 水位={} archive 水位={} lastEntryId={}",
                second.watermark("primary"), second.watermark("archive"), second.lastEntryId());

        assertEquals(2L, second.watermark("primary"));
        assertEquals(1L, second.watermark("archive"));
        assertEquals(3L, second.lastEntryId());

        List<Long> primaryResume = second.fetch("primary", 100).stream().map(StoredEntry::id).toList();
        List<Long> archiveResume = second.fetch("archive", 100).stream().map(StoredEntry::id).toList();
        log.info("[关键判定] 续传范围 primary={} archive={}（已确认部分不重发，未确认部分不丢失）",
                primaryResume, archiveResume);
        assertEquals(List.of(3L), primaryResume);
        assertEquals(List.of(2L, 3L), archiveResume);

        // 续号不回退
        long id4 = second.append(reading(4), 1000);
        assertEquals(4L, id4);

        // 重复/回退确认被忽略
        second.confirm("primary", 1L);
        assertEquals(2L, second.watermark("primary"));
    }

    @Test
    void capacityFull_blocksThenRejectsAfterTimeout() throws Exception {
        Path dir = Path.of("target/test-data/store-capacity");
        deleteRecursively(dir);
        PendingStore store = new PendingStore(props(dir, 3));
        store.recover();
        store.loadWatermarks(List.of("primary", "archive"));

        for (long seq = 1; seq <= 3; seq++) {
            assertTrue(store.append(reading(seq), 500) > 0);
        }
        log.info("[测试输入] 缓存已写满 {}/{}", store.pendingCount(), store.capacity());

        long start = System.currentTimeMillis();
        long rejectedId = store.append(reading(99), 300);
        long elapsed = System.currentTimeMillis() - start;
        log.info("[关键判定] 满容量写入 -> entryId={}（-1 表示显式背压拒绝）, 耗时={}ms",
                rejectedId, elapsed);
        assertEquals(-1L, rejectedId);
        assertTrue(elapsed >= 250, "应当等待接近超时时间，而不是静默丢弃");

        // 两消费方确认后腾出空间，新的写入成功
        List<StoredEntry> all = store.fetch("primary", 100);
        store.confirm("primary", all.get(0).id());
        store.confirm("archive", all.get(0).id());
        log.info("[关键判定] 确认腾出空间后 pending={}", store.pendingCount());
        assertTrue(store.append(reading(100), 500) > 0);
    }

    private static void deleteRecursively(Path path) throws java.io.IOException {
        if (!java.nio.file.Files.exists(path)) {
            return;
        }
        try (var paths = java.nio.file.Files.walk(path)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    java.nio.file.Files.deleteIfExists(p);
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
