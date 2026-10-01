package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟云端通道（传统单出口，非幂等）：
 * - 可通过 bringUp()/bringDown() 模拟通道抖动与恢复；
 * - 通道 down 时 send 返回 false，调用方不得 ack；
 * - 成功接收的批次记录在 received 中，供测试与本地验证核对；
 * - 可配置模拟时延，用于背压场景。
 *
 * <p>注：Spring 运行期已切换为
 * {@link com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout.IdempotentSimulatedChannel}
 * （按身份幂等、台账持久化）。本类保留供原有单出口单测直接构造使用。</p>
 */
public class SimulatedCloudChannel implements CloudChannel {

    private static final Logger log = LoggerFactory.getLogger(SimulatedCloudChannel.class);

    private final List<TelemetryPoint> received = new CopyOnWriteArrayList<>();
    private final AtomicLong sendAttempts = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private volatile boolean up = true;
    private volatile long latencyMillis = 0;

    @Override
    public boolean send(List<TelemetryPoint> batch) {
        sendAttempts.incrementAndGet();
        if (!up) {
            sendFailures.incrementAndGet();
            log.debug("channel down, batch of {} rejected", batch.size());
            return false;
        }
        if (latencyMillis > 0) {
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        received.addAll(batch);
        log.debug("channel accepted batch of {}, totalReceived={}", batch.size(), received.size());
        return true;
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

    public List<TelemetryPoint> received() {
        return List.copyOf(received);
    }

    public long sendAttempts() {
        return sendAttempts.get();
    }

    public long sendFailures() {
        return sendFailures.get();
    }

    public void reset() {
        received.clear();
        sendAttempts.set(0);
        sendFailures.set(0);
        latencyMillis = 0;
        up = true;
    }
}
