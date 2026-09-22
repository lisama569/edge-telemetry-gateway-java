package com.github.lisama569.edgetelemetrygatewayjava.forward;

import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地模拟云端通道的公共实现。
 *
 * 可由测试/HTTP 控制：
 * - 通道可用性（模拟断网与恢复）；
 * - 投递延迟（模拟慢通道）；
 * - 接下来若干次投递脚本化失败（确定性地触发重试，不使用随机失败）。
 *
 * 云端按 idempotencyKey 幂等：重复投递同一批次键时直接返回成功且不重复计入，
 * 因而网关重启/重复续传不会造成云端重复数据。
 */
public abstract class AbstractSimulatedCloudChannel implements CloudChannel {

    private static final Logger log = LoggerFactory.getLogger(AbstractSimulatedCloudChannel.class);

    private final String name;
    private volatile boolean available = true;
    private volatile long delayMs = 0;
    private int remainingFailures = 0;

    private final Set<String> receivedKeys = Collections.synchronizedSet(new LinkedHashSet<>());
    private final List<StoredEntry> receivedInOrder = Collections.synchronizedList(new ArrayList<>());

    protected AbstractSimulatedCloudChannel(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void deliver(String idempotencyKey, List<StoredEntry> batch) throws ChannelException {
        if (!available) {
            log.warn("[cloud:{}] 通道不可用, 拒绝投递 key={} 批大小={}", name, idempotencyKey, batch.size());
            throw new ChannelException("通道 " + name + " 当前不可用");
        }
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ChannelException("投递被中断");
            }
        }
        synchronized (receivedKeys) {
            if (remainingFailures > 0) {
                remainingFailures--;
                log.warn("[cloud:{}] 脚本化投递失败 key={} 剩余失败次数={}", name, idempotencyKey, remainingFailures);
                throw new ChannelException(name + " 脚本化失败");
            }
            if (!receivedKeys.add(idempotencyKey)) {
                log.info("[cloud:{}] 重复投递被云端幂等忽略 key={} 批大小={}（不重复计数）",
                        name, idempotencyKey, batch.size());
                return;
            }
            receivedInOrder.addAll(batch);
        }
        log.info("[cloud:{}] 投递成功 key={} 批大小={} 云端累计={} 条",
                name, idempotencyKey, batch.size(), receivedInOrder.size());
    }

    @Override
    public boolean available() {
        return available;
    }

    public void setAvailable(boolean available) {
        boolean old = this.available;
        this.available = available;
        log.info("[cloud:{}] 可用性切换 {} -> {}", name, old, available);
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    /** 接下来 n 次 deliver 调用确定性失败（用于重试场景）。 */
    public void failNextDeliveries(int n) {
        this.remainingFailures = n;
    }

    public List<StoredEntry> receivedInOrder() {
        synchronized (receivedKeys) {
            return new ArrayList<>(receivedInOrder);
        }
    }

    public Set<String> receivedKeys() {
        synchronized (receivedKeys) {
            return new LinkedHashSet<>(receivedKeys);
        }
    }

    public long receivedCount() {
        return receivedInOrder.size();
    }

    public void reset() {
        synchronized (receivedKeys) {
            receivedKeys.clear();
            receivedInOrder.clear();
            remainingFailures = 0;
        }
    }
}
