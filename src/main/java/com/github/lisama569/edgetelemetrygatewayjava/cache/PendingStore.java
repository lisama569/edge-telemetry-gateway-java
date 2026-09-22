package com.github.lisama569.edgetelemetrygatewayjava.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.edgetelemetrygatewayjava.config.GatewayProperties;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 文件型待转发缓存。
 *
 * 存储布局（dataDir 下）：
 * <ul>
 *   <li>wal.log：每行一条 JSON，格式 {"id":long,"r":Reading}，仅追加；</li>
 *   <li>offset-{consumer}.wm：单个 long，该消费方已确认到的 entryId（水位）；</li>
 * </ul>
 *
 * 语义保证：
 * <ul>
 *   <li>entryId 全局单调递增，重启后从 WAL 最大 id 续号；</li>
 *   <li>每个消费方独立水位，fetch 取水位之后的条目，confirm 单调推进水位；</li>
 *   <li>两个消费方都确认过的数据（水位最小值之前）才可在压缩时物理清除；</li>
 *   <li>容量 = 尚未被全部消费方确认的条目数；满时 append 阻塞等待空间，
 *       超过 offerTimeoutMs 仍无空间则返回 -1（背压拒绝，绝不静默丢弃）；</li>
 *   <li>所有写操作在同一把锁下完成，WAL 与水位先落盘再返回，崩溃恢复后
 *       已确认数据不重发、未确认数据不丢失。</li>
 * </ul>
 */
@Component
public class PendingStore {

    private static final Logger log = LoggerFactory.getLogger(PendingStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path dataDir;
    private final Path walFile;
    private final int capacity;
    private final boolean fsync;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition spaceAvailable = lock.newCondition();

    /** id -> Reading，按 id 排序；与 WAL 物理内容保持一致，仅含最低水位之后的条目。 */
    private final TreeMap<Long, Reading> index = new TreeMap<>();
    /** consumer -> 水位（已确认到的 entryId，初始 0）。 */
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();
    /** 已注册的全部消费方；未产生过确认的消费方按水位 0 参与安全边界计算。 */
    private final Set<String> registeredConsumers = new HashSet<>();

    private long nextId = 1;

    public PendingStore(GatewayProperties properties) {
        this.dataDir = Path.of(properties.getDataDir());
        this.walFile = dataDir.resolve("wal.log");
        this.capacity = properties.getCacheCapacity();
        this.fsync = properties.isFsync();
    }

    /**
     * 启动恢复：加载 WAL、水位，重建内存索引与续号起点。
     */
    public void recover() throws IOException {
        lock.lock();
        try {
            Files.createDirectories(dataDir);
            index.clear();
            long maxId = 0;
            if (Files.exists(walFile)) {
                for (String line : Files.readAllLines(walFile, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    JsonNode node = MAPPER.readTree(line);
                    long id = node.get("id").asLong();
                    JsonNode r = node.get("r");
                    Reading reading = new Reading(
                            r.get("sourceId").asText(),
                            r.get("pointId").asText(),
                            r.get("seq").asLong(),
                            r.get("timestampMs").asLong(),
                            r.get("value").asDouble());
                    index.put(id, reading);
                    maxId = Math.max(maxId, id);
                }
            }
            nextId = maxId + 1;
            if (!Files.exists(walFile)) {
                Files.createFile(walFile);
            }
            log.info("[store] 恢复完成: WAL 现存条目数={}, 续号起点={}, 水位={}",
                    index.size(), nextId, watermarks);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 追加一条读数。容量满时阻塞等待；超时返回 -1。
     */
    public long append(Reading reading, long timeoutMs) {
        lock.lock();
        try {
            long deadlineNs = System.nanoTime() + timeoutMs * 1_000_000L;
            while (pendingCountLocked() >= capacity) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0) {
                    log.warn("[store] 缓存已满且等待超时 capacity={}, pending={}, 背压拒绝 reading={}",
                            capacity, pendingCountLocked(), reading);
                    return -1L;
                }
                spaceAvailable.awaitNanos(remainingNs);
            }
            long id = nextId++;
            String line = MAPPER.writeValueAsString(
                    Map.of("id", id, "r", reading)) + System.lineSeparator();
            appendToWal(line);
            index.put(id, reading);
            log.info("[store] 落盘 entryId={} pending={}/{} reading={}",
                    id, pendingCountLocked(), capacity, reading);
            return id;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("append 被中断", e);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 写入失败: " + reading, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取指定消费方水位之后的下一批（按 entryId 升序，最多 maxBatch 条）。
     */
    public List<StoredEntry> fetch(String consumer, int maxBatch) {
        lock.lock();
        try {
            long wm = watermarks.getOrDefault(consumer, 0L);
            List<StoredEntry> batch = new ArrayList<>();
            for (Map.Entry<Long, Reading> e : index.tailMap(wm + 1, true).entrySet()) {
                batch.add(new StoredEntry(e.getKey(), e.getValue()));
                if (batch.size() >= maxBatch) {
                    break;
                }
            }
            log.debug("[store] fetch consumer={} watermark={} 取到={} 条", consumer, wm, batch.size());
            return batch;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 确认消费方已处理到 entryId（含）。水位只能单调推进，立即落盘，
     * 随后在两个消费方都越过旧条目时压缩 WAL。
     */
    public void confirm(String consumer, long entryId) throws IOException {
        lock.lock();
        try {
            long previous = watermarks.getOrDefault(consumer, 0L);
            if (entryId < previous) {
                log.warn("[store] 忽略回退的确认 consumer={} old={} new={}", consumer, previous, entryId);
                return;
            }
            if (entryId == previous) {
                return;
            }
            writeWatermark(consumer, entryId);
            watermarks.put(consumer, entryId);
            log.info("[store] 确认水位推进 consumer={} {} -> {}", consumer, previous, entryId);
            compactIfNeededLocked();
            spaceAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public long watermark(String consumer) {
        return watermarks.getOrDefault(consumer, 0L);
    }

    public int pendingCount() {
        lock.lock();
        try {
            return pendingCountLocked();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public long lastEntryId() {
        lock.lock();
        try {
            return nextId - 1;
        } finally {
            lock.unlock();
        }
    }

    // ---- 内部方法（调用方持锁，除文件 IO 辅助方法外） ----

    /**
     * 缓存占用条目数（容量按此口径控制）：以所有“已注册”消费方的最低水位
     * （最慢消费方，未确认按 0）为界。只要还有任一消费方需要转发，该条目
     * 就占用容量；慢通道长期不确认时容量不再释放，背压最终传导回采集侧。
     */
    private int pendingCountLocked() {
        return (int) index.tailMap(effectiveMinWatermarkLocked(), false).size();
    }

    /**
     * 压缩/删数的安全边界：所有“已注册”消费方的最低水位，
     * 从未确认过的注册消费方按 0 计——只要存在尚未确认的消费方，
     * 其待读数据就不会被物理删除。
     */
    private long effectiveMinWatermarkLocked() {
        return registeredConsumers.stream()
                .mapToLong(c -> watermarks.getOrDefault(c, 0L))
                .min().orElse(0L);
    }

    private void appendToWal(String line) throws IOException {
        try (FileChannel ch = FileChannel.open(walFile, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            ch.write(StandardCharsets.UTF_8.encode(line));
            if (fsync) {
                ch.force(false);
            }
        }
    }

    private void writeWatermark(String consumer, long entryId) throws IOException {
        Path file = dataDir.resolve("offset-" + consumer + ".wm");
        Path tmp = dataDir.resolve("offset-" + consumer + ".wm.tmp");
        Files.writeString(tmp, Long.toString(entryId), StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.READ)) {
            if (fsync) {
                ch.force(true);
            }
        }
        Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * 压缩 WAL 与内存索引：以所有已注册消费方的最低水位为唯一安全边界，
     * 仅移除 entryId ≤ minWatermark 的前缀。
     *
     * 关键安全前提：如果当前没有任何已注册消费方（例如启动时尚未调用
     * loadWatermarks），绝不物理删数——此时无法判断未来是否会有消费方
     * 需要这些数据。只要存在一个未确认（水位 0）的已注册消费方，
     * minWatermark 即为 0，同样不会删除任何条目。
     */
    private void compactIfNeededLocked() throws IOException {
        if (registeredConsumers.isEmpty()) {
            return;
        }
        long minWatermark = effectiveMinWatermarkLocked();
        if (minWatermark <= 0L || index.isEmpty() || index.firstKey() > minWatermark) {
            return;
        }
        Path compacted = dataDir.resolve("wal.log.compacted");
        try (var writer = Files.newBufferedWriter(compacted, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map.Entry<Long, Reading> e : index.tailMap(minWatermark, false).entrySet()) {
                writer.write(MAPPER.writeValueAsString(Map.of("id", e.getKey(), "r", e.getValue())));
                writer.newLine();
            }
        }
        Files.move(compacted, walFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        // 内存索引不裁剪：较慢消费方水位更低时，其待读区间仍保留在内存，
        // 可继续 fetch 续传；只有重启后才依赖 WAL（WAL 已保留全部未确认数据）。
        log.info("[store] WAL 压缩完成: 物理保留 entryId>{} 共 {} 条, 内存索引 {} 条, 水位={}",
                minWatermark, index.tailMap(minWatermark, false).size(), index.size(), watermarks);
    }

    /**
     * 启动时加载已存在的消费方水位文件。
     */
    public void loadWatermarks(List<String> consumers) throws IOException {
        lock.lock();
        try {
            for (String consumer : consumers) {
                registeredConsumers.add(consumer);
                Path file = dataDir.resolve("offset-" + consumer + ".wm");
                if (Files.exists(file)) {
                    String text = Files.readString(file, StandardCharsets.UTF_8).trim();
                    if (!text.isEmpty()) {
                        watermarks.put(consumer, Long.parseLong(text));
                    }
                }
            }
            // 崩溃可能发生在压缩之前，WAL 中或含已全部确认的前缀：
            // 按所有已注册消费方的最低水位裁剪，慢消费方待读的中间区段保留。
            long minWatermark = effectiveMinWatermarkLocked();
            index.headMap(minWatermark, true).clear();
            log.info("[store] 水位加载完成: {}, 内存索引待办条目={}", watermarks, index.size());
        } finally {
            lock.unlock();
        }
    }

}

