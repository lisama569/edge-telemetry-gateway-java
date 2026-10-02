package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout;

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

/**
 * 多下游共享的持久化有序队列（单个快照文件，临时文件 + 原子替换）。
 *
 * 一致性模型（修复“单侧故障 + 写满”导致的两侧永久分叉）：
 * - 同一份已接收数据只在共享队列入队一次，所有下游订阅者看到的是同一条入队序列，
 *   因此任意测点的任意序号，两个下游最终要么都收到、要么都不收到，条数与顺序一致；
 * - 每个下游持有独立确认游标（{@link Endpoint}），各自按自己的进度 peek/ack；
 *   队首由最慢的下游决定：只有所有订阅者都确认过的条目才会被裁剪，
 *   慢/停摆的下游欠下的数据始终在队列里等它恢复续传，不会被快的一侧“带走”或淘汰；
 * - 共享容量满（仍有下游未确认的条目数达到上限）时确定性地拒绝本次写入
 *   （{@link AppendStatus#REJECTED_FULL} + 计数 + WARN），不淘汰任何已入队数据，
 *   因而不会出现“一边照收、另一边悄悄丢掉更早数据”；被拒写入由采集侧返回可重试背压；
 * - 队列、各下游确认游标与单调计数写在<strong>同一张快照</strong>里、一次原子替换落盘：
 *   任意一次状态变更（入队/确认/裁剪）要么完整生效、要么完全不生效，进程在任意时刻被杀，
 *   重启后恢复到上一个一致快照——已确认不重发，未确认按各自游标续传
 *   （at-least-once，由下游幂等账本去重）。
 *
 * 与原单出口 {@code FileTelemetryBuffer} 的区别：单出口缓存满时淘汰最旧；
 * 共享扇出缓存面向“多个下游必须一致”，满时拒绝而非淘汰。
 */
public final class FileSharedFanOutBuffer {

    private static final Logger log = LoggerFactory.getLogger(FileSharedFanOutBuffer.class);
    private static final String SNAPSHOT_FILE = "fanout-snapshot.jsonl";

    public enum AppendStatus {
        APPENDED,
        /** 共享容量已满：拒绝本次写入（背压），不入队、不淘汰、计数可观测，调用方可重试。 */
        REJECTED_FULL
    }

    public record AppendOutcome(AppendStatus status) {
        static final AppendOutcome APPENDED = new AppendOutcome(AppendStatus.APPENDED);
        static final AppendOutcome REJECTED_FULL = new AppendOutcome(AppendStatus.REJECTED_FULL);
    }

    /** 快照首行：单调计数。 */
    private record Counters(long globalTrimIndex, long appendedTotal, long rejectedFull) {
    }

    /** 一个下游的确认游标（快照第二行起）。 */
    private record Cursor(String name, long acked) {
    }

    private final Path snapshotFile;
    private final int capacity;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    /** 仍至少被一个下游占用的连续队列（起点为 globalTrimIndex）。 */
    private final List<TelemetryPoint> queue = new ArrayList<>();
    /** 已被所有订阅者确认、已从 queue 移除的条目总数（队列起点的全局序号）。 */
    private long globalTrimIndex = 0;
    /** 累计成功入队总数（单调递增，只增不减）。 */
    private long appendedTotal = 0;
    /** 因容量满被拒绝的写入次数（运行期背压计数，随每次成功落盘的快照持久化）。 */
    private long rejectedFull = 0;

    /** name -> 该下游已确认的全局序号（已确认条数）。 */
    private final Map<String, Long> ackedByName = new LinkedHashMap<>();

    public FileSharedFanOutBuffer(Path dir, int capacity) {
        this.snapshotFile = dir.resolve(SNAPSHOT_FILE);
        this.capacity = capacity;
        try {
            Files.createDirectories(dir);
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init shared fan-out buffer at " + dir, e);
        }
        log.info("shared fan-out buffer initialized: dir={} capacity={} recoveredPending={} appendedTotal={} cursors={}",
                dir, capacity, queue.size(), appendedTotal, ackedByName);
    }

    private void load() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        List<String> lines = Files.readAllLines(snapshotFile, StandardCharsets.UTF_8);
        if (lines.isEmpty() || lines.getFirst().isBlank()) {
            return;
        }
        Counters counters = mapper.readValue(lines.getFirst(), Counters.class);
        globalTrimIndex = counters.globalTrimIndex();
        appendedTotal = counters.appendedTotal();
        rejectedFull = counters.rejectedFull();

        int i = 1;
        // 游标行：含 "name" 字段；其余为 TelemetryPoint。游标先于数据出现。
        while (i < lines.size() && !lines.get(i).isBlank() && lines.get(i).contains("\"name\"")) {
            Cursor c = mapper.readValue(lines.get(i), Cursor.class);
            ackedByName.put(c.name(), c.acked());
            i++;
        }
        for (; i < lines.size(); i++) {
            if (!lines.get(i).isBlank()) {
                queue.add(mapper.readValue(lines.get(i), TelemetryPoint.class));
            }
        }
    }

    /**
     * 注册一个下游消费者，返回其独立视图。可在重启后为同一名字重新订阅以恢复进度；
     * 新名字只允许在队列为空（无任何在途数据）时加入，从而天然纳入同一份对账序列。
     */
    public synchronized BufferEndpoint subscribe(String name) {
        if (!ackedByName.containsKey(name)) {
            if (appendedTotal > 0) {
                throw new IllegalStateException(
                        "new downstream '" + name + "' cannot join shared fan-out buffer with in-flight data "
                                + "(appendedTotal=" + appendedTotal + "); add downstreams before ingestion");
            }
            ackedByName.put(name, 0L);
            persist();
        }
        return new Endpoint(name);
    }

    /** 追加一条到共享队列尾部；满时确定性拒绝（不淘汰）。 */
    public synchronized AppendOutcome append(TelemetryPoint point) {
        if (queue.size() >= capacity) {
            rejectedFull++;
            log.warn("shared fan-out buffer full (capacity={}, pending={}): reject append {}/{} seq={}, "
                            + "no data evicted; slowest downstream must catch up; rejectedTotal={}",
                    capacity, queue.size(), point.sourceId() + "/" + point.pointId(),
                    point.pointId(), point.seq(), rejectedFull);
            // 拒绝路径不落盘：避免写满后调用方高频重试造成 IO 风暴；
            // rejectedFull 为运行期背压计数，会随下一次成功入队/确认的快照持久化。
            return AppendOutcome.REJECTED_FULL;
        }
        queue.add(point);
        appendedTotal++;
        persist();
        return AppendOutcome.APPENDED;
    }

    /** 推进某个下游的确认游标，并在所有下游都确认后裁剪队首；整次变更一张快照原子落盘。 */
    private synchronized void acknowledge(String name, int count) {
        long before = ackedByName.get(name);
        long after = Math.min(before + count, appendedTotal);
        if (after == before) {
            return;
        }
        ackedByName.put(name, after);

        long minAcked = appendedTotal;
        for (long acked : ackedByName.values()) {
            minAcked = Math.min(minAcked, acked);
        }
        int trim = (int) Math.min(minAcked - globalTrimIndex, queue.size());
        if (trim > 0) {
            queue.subList(0, trim).clear();
            globalTrimIndex += trim;
            log.debug("shared fan-out trimmed {} globally-acked entries, remaining={}", trim, queue.size());
        }
        persist();
    }

    private synchronized List<TelemetryPoint> peekFor(String name, int max) {
        long acked = ackedByName.get(name);
        int fromIndex = (int) Math.max(0L, acked - globalTrimIndex);
        int toIndex = Math.min(queue.size(), fromIndex + Math.max(0, max));
        return new ArrayList<>(queue.subList(fromIndex, toIndex));
    }

    private synchronized long pendingFor(String name) {
        return appendedTotal - ackedByName.get(name);
    }

    /** 仍至少被一个下游占用（未被所有下游确认）的条数。 */
    public synchronized long size() {
        return queue.size();
    }

    /** 累计成功入队总数。 */
    public synchronized long appendedTotal() {
        return appendedTotal;
    }

    /** 因容量满被拒绝的写入次数。 */
    public synchronized long rejectedFullCount() {
        return rejectedFull;
    }

    /** 分下游未确认条数，用于对账。 */
    public synchronized Map<String, Long> pendingByConsumer() {
        Map<String, Long> m = new LinkedHashMap<>();
        for (String name : ackedByName.keySet()) {
            m.put(name, appendedTotal - ackedByName.get(name));
        }
        return m;
    }

    /** 分下游已确认条数，用于对账。 */
    public synchronized Map<String, Long> ackedByConsumer() {
        return new LinkedHashMap<>(ackedByName);
    }

    /** 把计数+游标+队列写入同一张快照并原子替换：任意一次状态变更整体生效或整体不生效。 */
    private void persist() {
        try {
            Path tmp = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
            List<String> lines = new ArrayList<>(ackedByName.size() + queue.size() + 1);
            lines.add(mapper.writeValueAsString(
                    new Counters(globalTrimIndex, appendedTotal, rejectedFull)));
            for (Map.Entry<String, Long> e : ackedByName.entrySet()) {
                lines.add(mapper.writeValueAsString(new Cursor(e.getKey(), e.getValue())));
            }
            for (TelemetryPoint p : queue) {
                lines.add(mapper.writeValueAsString(p));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist shared fan-out snapshot to " + snapshotFile, e);
        }
    }

    /**
     * 单个下游在共享队列上的消费者视图。
     * 非线程安全约束：一个视图只被该下游自己的单线程转发循环使用（与 Forwarder 模型一致）。
     */
    private final class Endpoint implements BufferEndpoint {
        private final String name;

        Endpoint(String name) {
            this.name = name;
        }

        @Override
        public List<TelemetryPoint> peekBatch(int max) {
            return peekFor(name, max);
        }

        @Override
        public void ack(int count) {
            acknowledge(name, count);
        }

        @Override
        public long pending() {
            return pendingFor(name);
        }

        @Override
        public long ackedTotal() {
            synchronized (FileSharedFanOutBuffer.this) {
                return ackedByName.get(name);
            }
        }

        @Override
        public String name() {
            return name;
        }
    }
}
