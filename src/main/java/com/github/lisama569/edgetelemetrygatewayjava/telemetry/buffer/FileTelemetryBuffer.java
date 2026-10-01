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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于本地文件（JSON Lines）的持久化缓存。
 *
 * 语义：
 * - append：容量未满直接入队；已满时确定性地淘汰最旧一条（droppedCount 递增），行为可预期。
 * - peekBatch：只读查看队首批次，不删除。
 * - ack(count)：转发成功后确认，从队首移除并立即落盘 —— 重启后已确认数据不会重发。
 * - 未确认数据保留在文件中，重启后自动恢复续传（at-least-once）。
 * - 每次变更先写临时文件再原子替换，避免半写状态。
 */
public class FileTelemetryBuffer implements TelemetryBuffer {

    private static final Logger log = LoggerFactory.getLogger(FileTelemetryBuffer.class);
    private static final String DEFAULT_FILE_NAME = "buffer.jsonl";

    private final Path file;
    private final int capacity;
    private final Deque<TelemetryPoint> queue = new ArrayDeque<>();
    private final AtomicLong dropped = new AtomicLong();
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    /** 指定目录与容量，使用默认文件名 buffer.jsonl（兼容原单出口测试与行为）。 */
    public FileTelemetryBuffer(Path dir, int capacity) {
        this(dir.resolve(DEFAULT_FILE_NAME), capacity, Marker.FILE);
    }

    /** 指定完整文件路径，多个下游可各自使用独立文件与独立进度。 */
    public static FileTelemetryBuffer at(Path file, int capacity) {
        return new FileTelemetryBuffer(file, capacity, Marker.FILE);
    }

    /** 仅用于与公共 (Path,int) 目录构造器做签名消歧的私有标记。 */
    private enum Marker { FILE }

    private FileTelemetryBuffer(Path file, int capacity, Marker marker) {
        this.file = file;
        this.capacity = capacity;
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init buffer at " + file, e);
        }
        log.info("buffer initialized: file={} capacity={} recovered={}", file, capacity, queue.size());
    }

    private void load() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                queue.addLast(mapper.readValue(line, TelemetryPoint.class));
            }
        }
    }

    @Override
    public synchronized AppendOutcome append(TelemetryPoint point) {
        TelemetryPoint evicted = null;
        AppendStatus status = AppendStatus.APPENDED;
        if (queue.size() >= capacity) {
            evicted = queue.pollFirst();
            dropped.incrementAndGet();
            status = AppendStatus.APPENDED_EVICTED_OLDEST;
            log.warn("buffer overflow: capacity={} evicted oldest seq={} key={} droppedTotal={}",
                    capacity, evicted.seq(), evicted.key(), dropped.get());
        }
        queue.addLast(point);
        persist();
        return new AppendOutcome(status, evicted);
    }

    @Override
    public synchronized List<TelemetryPoint> peekBatch(int max) {
        List<TelemetryPoint> batch = new ArrayList<>(Math.min(max, queue.size()));
        for (TelemetryPoint p : queue) {
            if (batch.size() >= max) {
                break;
            }
            batch.add(p);
        }
        return batch;
    }

    @Override
    public synchronized void ack(int count) {
        for (int i = 0; i < count && !queue.isEmpty(); i++) {
            queue.pollFirst();
        }
        persist();
        log.debug("acked {} entries, remaining={}", count, queue.size());
    }

    @Override
    public synchronized long size() {
        return queue.size();
    }

    @Override
    public long droppedCount() {
        return dropped.get();
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            List<String> lines = new ArrayList<>(queue.size());
            for (TelemetryPoint p : queue) {
                lines.add(mapper.writeValueAsString(p));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist buffer to " + file, e);
        }
    }
}
