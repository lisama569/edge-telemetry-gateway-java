package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 转发器：单一消费循环从缓存队首取批次发往云端，成功后 ack，失败保留待重试。
 *
 * 背压与一致性语义：
 * - 采集侧写入只受缓存容量约束；容量满时由缓存确定性地淘汰最旧并计数，绝不静默丢弃。
 * - 单消费循环 + 队首 ack 保证转发顺序与缓存顺序一致，多轮重试/重启后结论可复现。
 * - 通道 down 或发送失败时不 ack，数据保留；状态通过计数器与日志可观测。
 */
@Component
public class Forwarder {

    private static final Logger log = LoggerFactory.getLogger(Forwarder.class);

    private final TelemetryBuffer buffer;
    private final CloudChannel channel;
    private final int batchSize;
    private final long intervalMillis;

    private final AtomicLong flushRounds = new AtomicLong();
    private final AtomicLong flushedTotal = new AtomicLong();
    private final AtomicLong failedRounds = new AtomicLong();
    private final AtomicLong skippedChannelDown = new AtomicLong();

    private ScheduledExecutorService executor;

    public Forwarder(TelemetryBuffer buffer,
                     CloudChannel channel,
                     @Value("${telemetry.forward.batch-size:100}") int batchSize,
                     @Value("${telemetry.forward.interval-millis:1000}") long intervalMillis) {
        this.buffer = buffer;
        this.channel = channel;
        this.batchSize = batchSize;
        this.intervalMillis = intervalMillis;
    }

    /** 启动后台转发循环（单线程，保证顺序）。 */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "telemetry-forwarder");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::flushOnceSafely, 0, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("forwarder started: batchSize={} intervalMillis={}", batchSize, intervalMillis);
    }

    public synchronized void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private void flushOnceSafely() {
        try {
            flushOnce();
        } catch (RuntimeException e) {
            failedRounds.incrementAndGet();
            log.error("flush round failed unexpectedly", e);
        }
    }

    /**
     * 尝试转发一轮。
     * @return 本轮成功确认条数；通道不可用或无数据时返回 0。
     */
    public int flushOnce() {
        flushRounds.incrementAndGet();
        if (!channel.isUp()) {
            skippedChannelDown.incrementAndGet();
            log.debug("channel down, skip flush (buffered={})", buffer.size());
            return 0;
        }
        List<TelemetryPoint> batch = buffer.peekBatch(batchSize);
        if (batch.isEmpty()) {
            return 0;
        }
        boolean ok;
        try {
            ok = channel.send(batch);
        } catch (RuntimeException e) {
            failedRounds.incrementAndGet();
            log.warn("channel send threw, batch of {} kept for retry: {}", batch.size(), e.getMessage());
            return 0;
        }
        if (!ok) {
            failedRounds.incrementAndGet();
            log.warn("channel rejected batch of {}, kept for retry (buffered={})", batch.size(), buffer.size());
            return 0;
        }
        buffer.ack(batch.size());
        flushedTotal.addAndGet(batch.size());
        log.debug("flushed {} entries, remaining={}", batch.size(), buffer.size());
        return batch.size();
    }

    public long flushRounds() {
        return flushRounds.get();
    }

    public long flushedTotal() {
        return flushedTotal.get();
    }

    public long failedRounds() {
        return failedRounds.get();
    }

    public long skippedChannelDown() {
        return skippedChannelDown.get();
    }
}
