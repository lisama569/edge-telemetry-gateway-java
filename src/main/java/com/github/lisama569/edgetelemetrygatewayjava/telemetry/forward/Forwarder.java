package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 转发器：单一消费循环从某个下游自己的缓存队首取批次发送，成功后 ack，失败保留待重试。
 *
 * 每个下游拥有独立的 Forwarder 实例与独立线程：一个下游变慢或失败只会阻塞自身循环，
 * 不会拖住另一个下游；各自按自己的缓存进度续传。
 *
 * 背压与一致性语义：
 * - 采集侧写入只受各缓存容量约束；容量满时由缓存确定性地淘汰最旧并计数，绝不静默丢弃。
 * - 单消费循环 + 队首 ack 保证转发顺序与缓存顺序一致，多轮重试/重启后结论可复现。
 * - 通道 down 或发送失败（返回 null/false 或抛异常）时不 ack，数据保留；状态由计数器与日志可观测。
 * - 重复触发 flush：空队返回 0，已确认数据不会被再次取出/发送。
 */
public class Forwarder {

    private static final Logger log = LoggerFactory.getLogger(Forwarder.class);

    private final String name;
    private final Source source;
    private final Predicate<List<TelemetryPoint>> send;
    private final java.util.function.BooleanSupplier up;
    private final int batchSize;
    private final long intervalMillis;

    /** 转发循环读取/确认的数据来源：屏蔽单出口缓存与共享扇出消费者视图的差异。 */
    private interface Source {
        List<TelemetryPoint> peekBatch(int max);

        void ack(int count);

        long size();
    }

    private final AtomicLong flushRounds = new AtomicLong();
    private final AtomicLong flushedTotal = new AtomicLong();
    private final AtomicLong failedRounds = new AtomicLong();
    private final AtomicLong skippedChannelDown = new AtomicLong();

    private ScheduledExecutorService executor;

    /** 新双下游路径：绑定共享扇出缓存上的某个下游消费者视图。 */
    public Forwarder(String name,
                     BufferEndpoint endpoint,
                     DownstreamChannel channel,
                     int batchSize,
                     long intervalMillis) {
        this(name,
                new Source() {
                    @Override
                    public List<TelemetryPoint> peekBatch(int max) {
                        return endpoint.peekBatch(max);
                    }

                    @Override
                    public void ack(int count) {
                        endpoint.ack(count);
                    }

                    @Override
                    public long size() {
                        return endpoint.pending();
                    }
                },
                batch -> channel.deliver(batch) != null,
                channel::isUp,
                batchSize,
                intervalMillis);
    }

    /** 新双下游路径：绑定一个具名下游通道与独立缓存。 */
    public Forwarder(String name,
                     TelemetryBuffer buffer,
                     DownstreamChannel channel,
                     int batchSize,
                     long intervalMillis) {
        this(name,
                new Source() {
                    @Override
                    public List<TelemetryPoint> peekBatch(int max) {
                        return buffer.peekBatch(max);
                    }

                    @Override
                    public void ack(int count) {
                        buffer.ack(count);
                    }

                    @Override
                    public long size() {
                        return buffer.size();
                    }
                },
                batch -> channel.deliver(batch) != null,
                channel::isUp,
                batchSize,
                intervalMillis);
    }

    /** 兼容原单出口路径：基于 {@link CloudChannel}，名称固定为 cloud。 */
    public Forwarder(TelemetryBuffer buffer,
                     CloudChannel channel,
                     int batchSize,
                     long intervalMillis) {
        this("cloud",
                new Source() {
                    @Override
                    public List<TelemetryPoint> peekBatch(int max) {
                        return buffer.peekBatch(max);
                    }

                    @Override
                    public void ack(int count) {
                        buffer.ack(count);
                    }

                    @Override
                    public long size() {
                        return buffer.size();
                    }
                },
                channel::send,
                channel::isUp,
                batchSize,
                intervalMillis);
    }

    private Forwarder(String name,
                      Source source,
                      Predicate<List<TelemetryPoint>> send,
                      java.util.function.BooleanSupplier up,
                      int batchSize,
                      long intervalMillis) {
        this.name = name;
        this.source = source;
        this.send = send;
        this.up = up;
        this.batchSize = batchSize;
        this.intervalMillis = intervalMillis;
    }

    public String name() {
        return name;
    }

    /** 启动后台转发循环（单线程，保证该下游内顺序）。 */
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "telemetry-forwarder-" + name);
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::flushOnceSafely, 0, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("forwarder[{}] started: batchSize={} intervalMillis={}", name, batchSize, intervalMillis);
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
        } catch (FatalForwardingCrash crash) {
            // 模拟进程死亡：停止调度，绝不 ack；真实系统里此处等价于进程终止。
            log.error("forwarder[{}] fatal crash after downstream delivery, ack suppressed; "
                    + "restart required to resume: {}", name, crash.getMessage());
            stop();
        } catch (RuntimeException e) {
            failedRounds.incrementAndGet();
            log.error("forwarder[{}] flush round failed unexpectedly", name, e);
        }
    }

    /**
     * 尝试转发一轮。
     * @return 本轮成功确认条数；通道不可用或无数据时返回 0。
     */
    public int flushOnce() {
        flushRounds.incrementAndGet();
        if (!up.getAsBoolean()) {
            skippedChannelDown.incrementAndGet();
            log.debug("forwarder[{}] channel down, skip flush (buffered={})", name, source.size());
            return 0;
        }
        List<TelemetryPoint> batch = source.peekBatch(batchSize);
        if (batch.isEmpty()) {
            return 0;
        }
        boolean ok;
        try {
            ok = send.test(batch);
        } catch (FatalForwardingCrash crash) {
            // 致命崩溃（如下游已接收但本地 ack 前断电）：绝不 ack，异常向上传播中止本轮，
            // 交由“重启续传”处理；普通 RuntimeException 仍按可重试失败处理。
            throw crash;
        } catch (RuntimeException e) {
            failedRounds.incrementAndGet();
            log.warn("forwarder[{}] channel send threw, batch of {} kept for retry: {}",
                    name, batch.size(), e.getMessage());
            return 0;
        }
        if (!ok) {
            failedRounds.incrementAndGet();
            log.warn("forwarder[{}] channel rejected batch of {}, kept for retry (buffered={})",
                    name, batch.size(), source.size());
            return 0;
        }
        source.ack(batch.size());
        flushedTotal.addAndGet(batch.size());
        log.debug("forwarder[{}] flushed {} entries, remaining={}", name, batch.size(), source.size());
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
