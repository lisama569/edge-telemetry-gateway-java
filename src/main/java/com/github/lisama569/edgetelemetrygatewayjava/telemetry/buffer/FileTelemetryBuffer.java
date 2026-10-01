package com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于本地文件（JSON Lines）的持久化缓存，支持传统单出口与多下游游标两种模式。
 *
 * 落盘文件：
 * - buffer.jsonl：每行一个 {offset, point} 信封，先写 .tmp 再原子替换，避免半写状态。
 * - progress.json：{nextOffset, consumers:{name: ackedOffset}}，消费者游标的权威进度。
 *
 * 语义：
 * - 传统模式（未注册消费者）：append 满时确定性淘汰最旧（droppedCount 递增）；ack(count) 移除队首。
 * - 多下游模式：append 满且队首仍被任一下游占用时确定性拒绝（REJECTED_BUFFER_FULL），
 *   任何下游都不会多收/漏收；各下游按自身游标 ack，慢下游不阻塞快下游消费，
 *   只有全部下游确认过的队首数据才物理移除。
 * - 崩溃恢复：buffer 与 progress 分别原子落盘；未确认数据重启后按各下游自身进度续传。
 */
@Component
public class FileTelemetryBuffer implements TelemetryBuffer {

    private static final Logger log = LoggerFactory.getLogger(FileTelemetryBuffer.class);
    private static final String FILE_NAME = "buffer.jsonl";
    private static final String PROGRESS_FILE_NAME = "progress.json";

    private record StoredEntry(long offset, TelemetryPoint point) {
    }

    private record Progress(long nextOffset, Map<String, Long> consumers) {
    }

    private final Path file;
    private final Path progressFile;
    private final int capacity;
    private final Deque<Envelope> queue = new ArrayDeque<>();
    private final Map<String, Long> consumerOffsets = new LinkedHashMap<>();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong rejectedFull = new AtomicLong();
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private long nextOffset = 0;

    public FileTelemetryBuffer(
            @Value("${telemetry.buffer.dir:./data/buffer}") Path dir,
            @Value("${telemetry.buffer.capacity:10000}") int capacity) {
        this.file = dir.resolve(FILE_NAME);
        this.progressFile = dir.resolve(PROGRESS_FILE_NAME);
        this.capacity = capacity;
        try {
            Files.createDirectories(dir);
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init buffer at " + dir, e);
        }
        log.info("buffer initialized: file={} capacity={} recovered={} consumers={}",
                file, capacity, queue.size(), consumerOffsets);
    }

    private void load() throws IOException {
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    StoredEntry e = mapper.readValue(line, StoredEntry.class);
                    queue.addLast(new Envelope(e.offset(), e.point()));
                    nextOffset = Math.max(nextOffset, e.offset() + 1);
                }
            }
        }
        if (Files.exists(progressFile)) {
            Progress p = mapper.readValue(Files.readString(progressFile, StandardCharsets.UTF_8), Progress.class);
            if (p.nextOffset() >= nextOffset) {
                nextOffset = p.nextOffset();
            }
            consumerOffsets.putAll(p.consumers());
        }
    }

    @Override
    public synchronized AppendOutcome append(TelemetryPoint point) {
        if (!consumerOffsets.isEmpty()) {
            // 多下游模式：队首仍被任一下游占用时不能淘汰（否则慢下游会漏收），确定性拒绝写入
            if (queue.size() >= capacity) {
                rejectedFull.incrementAndGet();
                dropped.incrementAndGet();
                log.warn("buffer full (capacity={}) with {} downstream(s) pending, reject point key={} seq={} "
                                + "rejectedFullTotal={}", capacity, consumerOffsets.size(),
                        point.key(), point.seq(), rejectedFull.get());
                return AppendOutcome.rejectedFull();
            }
        } else if (queue.size() >= capacity) {
            // 传统单出口模式：确定性淘汰最旧一条
            Envelope evictedEnv = queue.pollFirst();
            TelemetryPoint evicted = evictedEnv.point();
            dropped.incrementAndGet();
            log.warn("buffer overflow: capacity={} evicted oldest offset={} key={} seq={} droppedTotal={}",
                    capacity, evictedEnv.offset(), evicted.key(), evicted.seq(), dropped.get());
            long offset = nextOffset++;
            queue.addLast(new Envelope(offset, point));
            persistBuffer();
            persistProgress();
            return AppendOutcome.evictedOldest(evicted, offset);
        }
        long offset = nextOffset++;
        queue.addLast(new Envelope(offset, point));
        persistBuffer();
        persistProgress();
        return AppendOutcome.appended(offset);
    }

    @Override
    public synchronized List<TelemetryPoint> peekBatch(int max) {
        List<TelemetryPoint> batch = new ArrayList<>(Math.min(max, queue.size()));
        for (Envelope e : queue) {
            if (batch.size() >= max) {
                break;
            }
            batch.add(e.point());
        }
        return batch;
    }

    @Override
    public synchronized void ack(int count) {
        for (int i = 0; i < count && !queue.isEmpty(); i++) {
            queue.pollFirst();
        }
        persistBuffer();
        log.debug("legacy ack {} entries, remaining={}", count, queue.size());
    }

    @Override
    public synchronized void registerConsumer(String consumer) {
        if (consumerOffsets.putIfAbsent(consumer, -1L) == null) {
            persistProgress();
            log.info("registered downstream consumer '{}', bufferedPending={}", consumer, queue.size());
        }
    }

    @Override
    public synchronized List<Envelope> peekBatchFor(String consumer, int max) {
        Long cursor = requireCursor(consumer);
        List<Envelope> batch = new ArrayList<>(Math.min(max, queue.size()));
        for (Envelope e : queue) {
            if (batch.size() >= max) {
                break;
            }
            if (e.offset() > cursor) {
                batch.add(e);
            }
        }
        return batch;
    }

    @Override
    public synchronized void ack(String consumer, int count) {
        long cursor = requireCursor(consumer);
        long advanced = cursor;
        int applied = 0;
        for (Envelope e : queue) {
            if (applied >= count) {
                break;
            }
            if (e.offset() > advanced) {
                advanced = e.offset();
                applied++;
            }
        }
        consumerOffsets.put(consumer, advanced);
        persistProgress();

        // 所有消费者都确认过的队首数据才能物理移除，保证慢下游不漏收
        long minCursor = consumerOffsets.values().stream().mapToLong(Long::longValue).min().orElse(-1L);
        int trimmed = 0;
        while (!queue.isEmpty() && queue.peekFirst().offset() <= minCursor) {
            queue.pollFirst();
            trimmed++;
        }
        if (trimmed > 0) {
            persistBuffer();
        }
        log.debug("consumer '{}' ack {} entries -> cursor={}, physicalTrimmed={}, remaining={}",
                consumer, count, advanced, trimmed, queue.size());
    }

    @Override
    public synchronized long ackedOffset(String consumer) {
        return requireCursor(consumer);
    }

    private long requireCursor(String consumer) {
        Long cursor = consumerOffsets.get(consumer);
        if (cursor == null) {
            throw new IllegalArgumentException("consumer not registered: " + consumer);
        }
        return cursor;
    }

    @Override
    public synchronized long size() {
        return queue.size();
    }

    @Override
    public long droppedCount() {
        return dropped.get();
    }

    @Override
    public long rejectedFullCount() {
        return rejectedFull.get();
    }

    private void persistBuffer() {
        try {
            Path tmp = file.resolveSibling(FILE_NAME + ".tmp");
            List<String> lines = new ArrayList<>(queue.size());
            for (Envelope e : queue) {
                lines.add(mapper.writeValueAsString(new StoredEntry(e.offset(), e.point())));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist buffer to " + file, e);
        }
    }

    private void persistProgress() {
        try {
            Path tmp = progressFile.resolveSibling(PROGRESS_FILE_NAME + ".tmp");
            String json = mapper.writeValueAsString(new Progress(nextOffset, new LinkedHashMap<>(consumerOffsets)));
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, progressFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist progress to " + progressFile, e);
        }
    }
}
