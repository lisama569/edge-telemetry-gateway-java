package com.github.lisama569.edgetelemetrygatewayjava.forward;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.config.GatewayProperties;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 转发调度服务。
 *
 * 设计要点：
 * <ul>
 *   <li>每个云端通道一个专属单线程 worker；同消费方严格按 entryId FIFO，
 *       顺序结论在任何抖动/重启下都可复现；</li>
 *   <li>每批使用确定性的幂等键 consumer:startId-endId，重复续传由云端去重，
 *       确认后水位先落盘，因此已确认数据不重发、未确认数据不丢失；</li>
 *   <li>通道不可用或投递失败：worker 退避等待并周期性探测，恢复后从水位续传；
 *       慢通道只阻塞本消费方 worker，不影响采集侧（缓存满则走显式背压拒绝）；</li>
 *   <li>通道故障次数、重试次数均计入 metrics，提供明确的可观测判定。</li>
 * </ul>
 */
@Service
public class ForwardingService {

    private static final Logger log = LoggerFactory.getLogger(ForwardingService.class);
    private static final long BACKOFF_MS = 200;

    private final PendingStore store;
    private final GatewayProperties properties;
    private final GatewayMetrics metrics;
    private final List<CloudChannel> channels;

    private ExecutorService pool;
    private final List<WorkerControl> controls = new ArrayList<>();
    private volatile boolean running;
    /** 暂停开关：暂停后 worker 只轮询不投递（用于测试恢复后的断言时刻）。 */
    private volatile boolean paused;

    public ForwardingService(PendingStore store, GatewayProperties properties,
                             GatewayMetrics metrics, List<CloudChannel> channels) {
        this.store = store;
        this.properties = properties;
        this.metrics = metrics;
        this.channels = channels;
        this.paused = properties.isStartPaused();
    }

    /** 暂停转发（不影响采集与落盘）。 */
    public void pause() {
        paused = true;
        log.info("[forward] 转发已暂停");
    }

    /** 恢复转发并立即唤醒所有 worker。 */
    public void resume() {
        paused = false;
        kick();
        log.info("[forward] 转发已恢复");
    }

    private final ReentrantLock drainLock = new ReentrantLock();
    private final Condition drained = drainLock.newCondition();

    @PostConstruct
    public void start() {
        running = true;
        try {
            List<String> names = channels.stream().map(CloudChannel::name).toList();
            store.recover();
            store.loadWatermarks(names);
        } catch (IOException e) {
            throw new UncheckedIOException("缓存恢复失败", e);
        }
        pool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("forwarder-", 0).factory());
        for (CloudChannel channel : channels) {
            WorkerControl control = new WorkerControl(channel);
            controls.add(control);
            pool.submit(() -> runWorker(channel, control));
        }
        log.info("[forward] 转发服务已启动, 消费方={}", channels.stream().map(CloudChannel::name).toList());
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (pool == null) {
            return;
        }
        controls.forEach(WorkerControl::shutdown);
        pool.shutdownNow();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("[forward] 转发服务已停止");
    }

    /**
     * 通道恢复后主动唤醒 worker（HTTP 切换可用性时调用）。
     */
    public void kick() {
        controls.forEach(WorkerControl::kick);
    }

    /**
     * 阻塞等待指定消费方水位推进到 entryId（含）。
     */
    public boolean awaitDrained(String consumer, long entryId, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        drainLock.lock();
        try {
            while (store.watermark(consumer) < entryId) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                drained.awaitNanos(remaining);
            }
            return true;
        } finally {
            drainLock.unlock();
        }
    }

    public Map<String, Object> status() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (CloudChannel c : channels) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("available", c.available());
            info.put("watermark", store.watermark(c.name()));
            map.put(c.name(), info);
        }
        return map;
    }

    private void runWorker(CloudChannel channel, WorkerControl control) {
        String consumer = channel.name();
        log.info("[forward:{}] worker 启动", consumer);
        while (!control.stopped) {
            try {
                List<StoredEntry> batch = store.fetch(consumer, properties.getFetchBatchSize());
                if (batch.isEmpty()) {
                    control.awaitWork(BACKOFF_MS);
                    continue;
                }
                if (paused) {
                    log.debug("[forward:{}] 已暂停, 暂不投递, 待投递首条 entryId={}",
                            consumer, batch.get(0).id());
                    control.awaitWork(BACKOFF_MS);
                    continue;
                }
                if (!channel.available()) {
                    metrics.channelFailures.incrementAndGet();
                    log.info("[forward:{}] 通道不可用, 暂存等待恢复, 待投递首条 entryId={}",
                            consumer, batch.get(0).id());
                    control.awaitWork(BACKOFF_MS);
                    continue;
                }
                long firstId = batch.get(0).id();
                long lastId = batch.get(batch.size() - 1).id();
                if (!running || control.stopped) {
                    return;
                }
                String key = consumer + ":" + firstId + "-" + lastId;
                try {
                    channel.deliver(key, batch);
                } catch (CloudChannel.ChannelException e) {
                    metrics.retryAttempts.incrementAndGet();
                    metrics.channelFailures.incrementAndGet();
                    log.warn("[forward:{}] 投递失败 key={} cause={}, 退避 {}ms 后续传(整批重发, 云端幂等去重)",
                            consumer, key, e.getMessage(), BACKOFF_MS);
                    control.awaitWork(BACKOFF_MS);
                    continue;
                }
                if (!running) {
                    // 关闭信号在投递期间到达：不推进水位，重启后整批安全重发
                    return;
                }
                store.confirm(consumer, lastId);
                if ("primary".equals(consumer)) {
                    metrics.deliveredPrimary.addAndGet(batch.size());
                } else if ("archive".equals(consumer)) {
                    metrics.deliveredArchive.addAndGet(batch.size());
                }
                log.info("[forward:{}] 确认完成 key={} 条数={} 水位={}",
                        consumer, key, batch.size(), lastId);
                drainLock.lock();
                try {
                    drained.signalAll();
                } finally {
                    drainLock.unlock();
                }
            } catch (IOException e) {
                log.error("[forward:{}] 水位确认失败, 停止该 worker 以防丢数", consumer, e);
                control.awaitWork(BACKOFF_MS);
            } catch (RuntimeException e) {
                log.error("[forward:{}] worker 未预期异常", consumer, e);
                control.awaitWork(BACKOFF_MS);
            }
        }
        log.info("[forward:{}] worker 退出", consumer);
    }

    private static final class WorkerControl {
        private final CloudChannel channel;
        private volatile boolean stopped;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition wake = lock.newCondition();

        private WorkerControl(CloudChannel channel) {
            this.channel = channel;
        }

        private void awaitWork(long ms) {
            lock.lock();
            try {
                if (stopped) {
                    return;
                }
                wake.await(ms, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }

        private void kick() {
            lock.lock();
            try {
                wake.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private void shutdown() {
            stopped = true;
            kick();
        }
    }
}
