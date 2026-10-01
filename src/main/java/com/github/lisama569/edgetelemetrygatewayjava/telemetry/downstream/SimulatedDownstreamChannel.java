package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟下游通道基类：可 up/down、可配置时延；内置按身份去重的幂等接收方。
 *
 * - down 时 send 返回 null（调用方不得 ack），不触碰幂等账本；
 * - up 时逐点交给 {@link IdempotentReceiver} 判定，返回与输入一一对应的判定结果；
 * - received 仅记录“真正接收”（ACCEPTED/LATE）的去重后数据，供本地对账。
 */
public abstract class SimulatedDownstreamChannel implements DownstreamChannel {

    private final String name;
    private final IdempotentReceiver receiver;
    private final List<TelemetryPoint> received = new CopyOnWriteArrayList<>();
    private final AtomicLong sendAttempts = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private volatile boolean up = true;
    private volatile long latencyMillis = 0;

    protected SimulatedDownstreamChannel(String name, IdempotentReceiver receiver) {
        this.name = name;
        this.receiver = receiver;
    }

    @Override
    public DeliveryBatchResult deliver(List<TelemetryPoint> batch) {
        sendAttempts.incrementAndGet();
        if (!up) {
            sendFailures.incrementAndGet();
            LoggerFactory.getLogger(getClass()).debug(
                    "downstream[{}] down, batch of {} rejected", name, batch.size());
            return null;
        }
        if (latencyMillis > 0) {
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        List<DeliveryResult> results = receiver.deliver(batch);
        for (int i = 0; i < batch.size(); i++) {
            if (results.get(i).verdict() != DeliveryVerdict.DUPLICATE) {
                received.add(batch.get(i));
            }
        }
        LoggerFactory.getLogger(getClass()).info(
                "downstream[{}] accepted batch of {} (uniqueReceived={})",
                name, batch.size(), received.size());
        return new DeliveryBatchResult(results);
    }

    @Override
    public boolean isUp() {
        return up;
    }

    public void bringUp() {
        up = true;
    }

    public void bringDown() {
        up = false;
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    public String name() {
        return name;
    }

    /** 真正接收到的去重后数据（ACCEPTED + LATE）。 */
    public List<TelemetryPoint> received() {
        return List.copyOf(received);
    }

    public long sendAttempts() {
        return sendAttempts.get();
    }

    public long sendFailures() {
        return sendFailures.get();
    }

    public IdempotentReceiver receiver() {
        return receiver;
    }

    public void reset() {
        received.clear();
        sendAttempts.set(0);
        sendFailures.set(0);
        latencyMillis = 0;
        up = true;
    }

    protected Logger log() {
        return LoggerFactory.getLogger(getClass());
    }
}
