package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多下游并行转发器。
 *
 * 一致性与隔离语义：
 * - 每个下游一个独立的单线程消费循环，只推进自己的游标（buffer.ack(name,n)），
 *   某下游变慢、失败、通道 down 时不 ack，数据保留按自身进度续传，既不拖住其他下游，
 *   也不会让其他下游多收/漏收；所有下游消费同一条有界队列，天然按同一 offset 顺序消费。
 * - 重复触发 flush（包括重启后重放）不会重复投递：游标只在下游整批处理成功后推进，
 *   重放批次由下游按身份幂等判重，最终每个身份只真正接收一次。
 */
public class FanOutForwarder {

    private static final Logger log = LoggerFactory.getLogger(FanOutForwarder.class);

    private final TelemetryBuffer buffer;
    private final Map<String, IdempotentSimulatedChannel> channels = new LinkedHashMap<>();
    private final int batchSize;
    private final long intervalMillis;

    private final Map<String, AtomicLong> flushedTotals = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> duplicateDeliveries = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> failedRounds = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> skippedDown = new ConcurrentHashMap<>();

    private final Map<String, ScheduledExecutorService> executors = new ConcurrentHashMap<>();

    public FanOutForwarder(TelemetryBuffer buffer,
                           List<Downstream> downstreams,
                           int batchSize,
                           long intervalMillis) {
        this.buffer = buffer;
        this.batchSize = batchSize;
        this.intervalMillis = intervalMillis;
        for (Downstream d : downstreams) {
            channels.put(d.name(), d.channel());
            flushedTotals.put(d.name(), new AtomicLong());
            duplicateDeliveries.put(d.name(), new AtomicLong());
            failedRounds.put(d.name(), new AtomicLong());
            skippedDown.put(d.name(), new AtomicLong());
            buffer.registerConsumer(d.name());
        }
    }

    /** 为每个下游启动独立消费循环。 */
    public synchronized void start() {
        for (String name : channels.keySet()) {
            if (executors.containsKey(name)) {
                continue;
            }
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fanout-forwarder-" + name);
                t.setDaemon(true);
                return t;
            });
            executor.scheduleWithFixedDelay(
                    () -> flushOnceSafely(name), 0, intervalMillis, TimeUnit.MILLISECONDS);
            executors.put(name, executor);
        }
        log.info("fan-out forwarder started for {} downstream(s): batchSize={} intervalMillis={}",
                channels.size(), batchSize, intervalMillis);
    }

    public synchronized void stop() {
        executors.values().forEach(ScheduledExecutorService::shutdownNow);
        executors.clear();
    }

    private void flushOnceSafely(String name) {
        try {
            flushOnce(name);
        } catch (RuntimeException e) {
            failedRounds.get(name).incrementAndGet();
            // 典型如 IdempotentSimulatedChannel.SimulatedCrash：游标未推进，重启/下轮重放
            log.warn("downstream '{}' flush crashed/failed: {} (cursor kept at offset={}, will replay)",
                    name, e.getMessage(), buffer.ackedOffset(name));
        }
    }

    /**
     * 尝试转发一轮。
     * @return 本轮新推进确认的条数；通道不可用/无数据/失败/崩溃时返回 0。
     */
    public int flushOnce(String name) {
        IdempotentSimulatedChannel channel = channels.get(name);
        if (channel == null) {
            throw new IllegalArgumentException("unknown downstream: " + name);
        }
        if (!channel.isUp()) {
            skippedDown.get(name).incrementAndGet();
            log.debug("downstream '{}' down, skip flush (cursor={}, buffered={})",
                    name, buffer.ackedOffset(name), buffer.size());
            return 0;
        }
        List<TelemetryBuffer.Envelope> batch = buffer.peekBatchFor(name, batchSize);
        if (batch.isEmpty()) {
            return 0;
        }
        IdempotentSimulatedChannel.SendResult result =
                channel.sendBatch(batch.stream().map(TelemetryBuffer.Envelope::point).toList());
        // 只有整批处理完成（含判重）才推进游标；中途抛异常时不 ack，下轮重放
        buffer.ack(name, batch.size());
        flushedTotals.get(name).addAndGet(batch.size());
        duplicateDeliveries.get(name).addAndGet(result.duplicateCount());
        log.info("downstream '{}' flushed: attempted={} newlyAccepted={} duplicates={} cursor={} buffered={}",
                name, batch.size(), result.acceptedCount(), result.duplicateCount(),
                buffer.ackedOffset(name), buffer.size());
        return batch.size();
    }

    public long ackedOffset(String name) {
        return buffer.ackedOffset(name);
    }

    public long flushedTotal(String name) {
        return flushedTotals.get(name).get();
    }

    public long duplicateDeliveries(String name) {
        return duplicateDeliveries.get(name).get();
    }

    public long failedRounds(String name) {
        return failedRounds.get(name).get();
    }

    public long skippedDown(String name) {
        return skippedDown.get(name).get();
    }

    public java.util.Set<String> downstreamNames() {
        return java.util.Set.copyOf(channels.keySet());
    }

    public IdempotentSimulatedChannel channel(String name) {
        return channels.get(name);
    }
}
