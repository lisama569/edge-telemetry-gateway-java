package com.github.lisama569.edgetelemetrygatewayjava.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 网关全链路计数器：
 * received（进入采集）、accepted（归一+去重后接受）、rejected（归一拒绝）、
 * duplicate、stale（去重判定）、backpressureRejected（缓存满超时）、
 * persisted（落盘）、deliveredPrimary/deliveredArchive（各消费方已确认）、
 * retryAttempts（投递重试）、channelFailures（通道不可用/失败观察次数）。
 */
@Component
public class GatewayMetrics {

    public final AtomicLong received = new AtomicLong();
    public final AtomicLong accepted = new AtomicLong();
    public final AtomicLong rejected = new AtomicLong();
    public final AtomicLong duplicate = new AtomicLong();
    public final AtomicLong stale = new AtomicLong();
    public final AtomicLong backpressureRejected = new AtomicLong();
    public final AtomicLong persisted = new AtomicLong();
    public final AtomicLong deliveredPrimary = new AtomicLong();
    public final AtomicLong deliveredArchive = new AtomicLong();
    public final AtomicLong retryAttempts = new AtomicLong();
    public final AtomicLong channelFailures = new AtomicLong();

    public Map<String, Object> snapshot() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("received", received.get());
        map.put("accepted", accepted.get());
        map.put("rejected", rejected.get());
        map.put("duplicate", duplicate.get());
        map.put("stale", stale.get());
        map.put("backpressureRejected", backpressureRejected.get());
        map.put("persisted", persisted.get());
        map.put("deliveredPrimary", deliveredPrimary.get());
        map.put("deliveredArchive", deliveredArchive.get());
        map.put("retryAttempts", retryAttempts.get());
        map.put("channelFailures", channelFailures.get());
        return map;
    }

    public void reset() {
        received.set(0);
        accepted.set(0);
        rejected.set(0);
        duplicate.set(0);
        stale.set(0);
        backpressureRejected.set(0);
        persisted.set(0);
        deliveredPrimary.set(0);
        deliveredArchive.set(0);
        retryAttempts.set(0);
        channelFailures.set(0);
    }
}
