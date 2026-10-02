package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 双下游共享的准入日志（fan-out coordinator）。
 *
 * 设计要点（修复“单侧故障/变慢 + 缓存写满”时两侧分叉）：
 * - 所有被采集侧接受的数据进入<strong>同一条</strong>全局有序日志，占用同一个容量上限；
 * - 每个下游只持有一个独立的“已确认游标”（{@link PerDownstreamView}），
 *   各读各的窗口、各 ack 各的进度，慢侧/故障侧绝不阻塞快侧读取它已经能拿到的数据；
 * - 容量判定针对整条数据：日志满（最旧一条尚未被全部下游确认）时，
 *   新数据被<strong>整条拒绝</strong>（{@link AppendStatus#REJECTED_FULL}），
 *   两侧一致地都不收这条；绝不再做“只淘汰某一侧最旧数据”，因此不可能出现
 *   “同一测点序号一边有、一边没有”；
 * - 只有当最旧条目已被<strong>所有</strong>下游确认后才允许整体推进并腾出容量
 *   （队列压缩，物理保留每个下游仍未确认的窗口）；
 * - 日志与每侧游标均落盘，进程重启后各下游按自己的游标续传未确认数据，
 *   已确认数据不会重发。
 *
 * 并发：append / 视图操作均在 coordinator 单一锁下串行；并发写入只影响全局顺序，
 * 每个下游看到的全局顺序完全相同。
 */
public class SharedFanOutBuffer {

    private static final Logger log = LoggerFactory.getLogger(SharedFanOutBuffer.class);
    private static final String LOG_FILE = "fanout-log.jsonl";

    private final Path dir;
    private final Path logFile;
    private final int capacity;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Object lock = new Object();

    /** 物理保留窗口：全局最旧未确认条目起的有序条目。 */
    private final List<Entry> window = new ArrayList<>();
    /** 逻辑游标（单调递增，重启后通过游标文件恢复）。 */
    private long baseSeq;
    private final LinkedHashMap<String, Cursor> cursors = new LinkedHashMap<>();

    private final AtomicLong rejected = new AtomicLong();

    private record Entry(long seq, TelemetryPoint point) {
    }

    /**
     * 创建共享准入日志。
     * @param dir      日志与每侧游标的落盘目录
     * @param capacity 共享容量（按“最旧未被全部下游确认”的条目计数）
     * @param names    固定顺序的下游名（cloud 在前）
     */
    public static SharedFanOutBuffer create(Path dir, int capacity, List<String> names) {
        return new SharedFanOutBuffer(dir, capacity, List.copyOf(names));
    }

    private static final class Cursor {
        long ackedSeq; // 该下游已确认到的逻辑序号（-1 表示一条都没确认）

        Cursor(long ackedSeq) {
            this.ackedSeq = ackedSeq;
        }
    }

    private SharedFanOutBuffer(Path dir, int capacity, List<String> names) {        this.dir = dir;
        this.logFile = dir.resolve(LOG_FILE);
        this.capacity = capacity;
        try {
            Files.createDirectories(dir);
            for (String name : names) {
                cursors.put(name, new Cursor(loadCursor(name)));
            }
            loadLog();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init shared fan-out buffer at " + dir, e);
        }
        log.info("shared fan-out buffer initialized: dir={} capacity={} retained={} cursors={}",
                dir, capacity, window.size(), describeCursors());
    }

    private Map<String, Long> describeCursors() {
        synchronized (lock) {
            Map<String, Long> m = new LinkedHashMap<>();
            for (Map.Entry<String, Cursor> e : cursors.entrySet()) {
                m.put(e.getKey(), e.getValue().ackedSeq);
            }
            return m;
        }
    }

    private Path cursorFile(String name) {
        return dir.resolve("cursor-" + name + ".properties");
    }

    private long loadCursor(String name) throws IOException {
        Path f = cursorFile(name);
        if (!Files.exists(f)) {
            return -1L;
        }
        for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            String t = line.trim();
            if (t.startsWith("ackedSeq=")) {
                return Long.parseLong(t.substring("ackedSeq=".length()).trim());
            }
        }
        return -1L;
    }

    private void loadLog() throws IOException {
        if (!Files.exists(logFile)) {
            return;
        }
        long minCursor = minAckedSeq();
        long maxSeq = minCursor;
        for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            StoredRecord rec = mapper.readValue(line, StoredRecord.class);
            if (rec.seq > minCursor) {
                window.add(new Entry(rec.seq, rec.point));
                maxSeq = Math.max(maxSeq, rec.seq);
            }
        }
        if (!window.isEmpty()) {
            baseSeq = window.get(0).seq();
        } else {
            baseSeq = maxSeq + 1;
        }
    }

    private long minAckedSeq() {
        long min = Long.MAX_VALUE;
        for (Cursor c : cursors.values()) {
            min = Math.min(min, c.ackedSeq);
        }
        return min == Long.MAX_VALUE ? -1L : min;
    }

    /**
     * 整条准入：容量满则两侧一致拒绝。
     * 成功时给该条目分配一个全局单调序号并追加到日志尾部。
     */
    public TelemetryBuffer.AppendOutcome append(TelemetryPoint point) {
        synchronized (lock) {
            compactIfAllAcked();
            if (window.size() >= capacity) {
                rejected.incrementAndGet();
                log.warn("shared fan-out buffer full (capacity={}, slowestDownstreamAcked={}), "
                                + "new point rejected for ALL downstreams: key={} seq={} rejectedTotal={}",
                        capacity, minAckedSeq(), point.key(), point.seq(), rejected.get());
                return new TelemetryBuffer.AppendOutcome(
                        TelemetryBuffer.AppendStatus.REJECTED_FULL, null);
            }
            long seq = window.isEmpty() ? baseSeq : window.get(window.size() - 1).seq() + 1L;
            window.add(new Entry(seq, point));
            appendLog(new StoredRecord(seq, point));
            log.debug("shared append: globalSeq={} key={} pointSeq={} retained={}",
                    seq, point.key(), point.seq(), window.size());
            return new TelemetryBuffer.AppendOutcome(TelemetryBuffer.AppendStatus.APPENDED, null);
        }
    }

    /** 所有最旧条目都被全部下游确认后，推进 baseSeq 并压缩窗口，腾出容量。 */
    private void compactIfAllAcked() {
        long min = minAckedSeq();
        int cut = 0;
        while (cut < window.size() && window.get(cut).seq() <= min) {
            cut++;
        }
        if (cut > 0) {
            window.subList(0, cut).clear();
            baseSeq = window.isEmpty() ? min + 1L : window.get(0).seq();
            rewriteLog();
        }
    }

    /** 每个下游独立的缓存视图：独立游标，读取/确认互不相干。 */
    public PerDownstreamView view(String name) {
        synchronized (lock) {
            Cursor c = cursors.get(name);
            if (c == null) {
                throw new IllegalArgumentException("no such downstream view: " + name);
            }
            return new PerDownstreamView(name, c);
        }
    }

    public long rejectedCount() {
        return rejected.get();
    }

    public int capacity() {
        return capacity;
    }

    public List<String> names() {
        synchronized (lock) {
            return List.copyOf(cursors.keySet());
        }
    }

    /**
     * 单个下游看到的“自己的缓存”：只暴露该下游尚未确认的全局日志后缀，
     * ack 只推进自己的游标；所有视图共享同一批底层条目，故序列必然一致。
     */
    public final class PerDownstreamView implements TelemetryBuffer {

        private final String name;
        private final Cursor cursor;

        private PerDownstreamView(String name, Cursor cursor) {
            this.name = name;
            this.cursor = cursor;
        }

        public String viewName() {
            return name;
        }

        @Override
        public TelemetryBuffer.AppendOutcome append(TelemetryPoint point) {
            return SharedFanOutBuffer.this.append(point);
        }

        @Override
        public List<TelemetryPoint> peekBatch(int max) {
            synchronized (lock) {
                List<TelemetryPoint> batch = new ArrayList<>(Math.min(max, Math.max(0, window.size())));
                for (Entry e : window) {
                    if (batch.size() >= max) {
                        break;
                    }
                    if (e.seq() > cursor.ackedSeq) {
                        batch.add(e.point());
                    }
                }
                return batch;
            }
        }

        @Override
        public void ack(int count) {
            synchronized (lock) {
                int acked = 0;
                long lastSeq = cursor.ackedSeq;
                for (Entry e : window) {
                    if (acked >= count) {
                        break;
                    }
                    if (e.seq() > cursor.ackedSeq) {
                        lastSeq = e.seq();
                        acked++;
                    }
                }
                if (acked > 0) {
                    cursor.ackedSeq = lastSeq;
                    persistCursor();
                    log.debug("downstream[{}] acked {} entries (ackedSeq={}, retained={})",
                            name, acked, lastSeq, window.size());
                    compactIfAllAcked();
                }
            }
        }

        @Override
        public long size() {
            synchronized (lock) {
                long n = 0;
                for (Entry e : window) {
                    if (e.seq() > cursor.ackedSeq) {
                        n++;
                    }
                }
                return n;
            }
        }

        /** 双下游路径不做单侧淘汰；真正“未送达”的条目是整条拒绝，计在 rejectedCount。 */
        @Override
        public long droppedCount() {
            return 0L;
        }

        @Override
        public long appendedCount() {
            synchronized (lock) {
                long tail = window.isEmpty() ? baseSeq - 1L : window.get(window.size() - 1).seq();
                return Math.max(0L, tail + 1L);
            }
        }

        @Override
        public long rejectedCount() {
            return SharedFanOutBuffer.this.rejectedCount();
        }

        private void persistCursor() {
            try {
                Path f = cursorFile(name);
                Path tmp = f.resolveSibling(f.getFileName() + ".tmp");
                Files.writeString(tmp, "ackedSeq=" + cursor.ackedSeq + System.lineSeparator(),
                        StandardCharsets.UTF_8);
                Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to persist cursor for " + name, e);
            }
        }
    }

    // ---- 持久化：全局日志 JSON Lines（seq + point），追加写；压缩时整文件重写 ----

    private record StoredRecord(long seq, TelemetryPoint point) {
    }

    private void appendLog(StoredRecord rec) {
        try {
            Files.writeString(logFile, mapper.writeValueAsString(rec) + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append shared log at " + logFile, e);
        }
    }

    private void rewriteLog() {
        try {
            Path tmp = logFile.resolveSibling(logFile.getFileName() + ".tmp");
            List<String> lines = new ArrayList<>(window.size());
            for (Entry e : window) {
                lines.add(mapper.writeValueAsString(new StoredRecord(e.seq(), e.point())));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to rewrite shared log at " + logFile, e);
        }
    }
}
