package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DeliveryBatchResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.FatalForwardingCrash;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用崩溃点装饰器：在被装饰通道真正完成一次投递之后、调用方 ack 之前抛出
 * {@link GatewayCrashed}，模拟进程在转发中途断电/被杀。
 *
 * 关键语义：此时数据已经到达下游（身份账本已持久化），但网关缓存尚未 ack，
 * 重启续传会再次投递同一批数据 —— 下游必须按身份判为 DUPLICATE，不能真正接收第二次。
 */
public class CrashBeforeAckChannel implements DownstreamChannel {

    /** 标记“进程已死”：与普通发送失败区分，转发器不得 ack，重启后续传。 */
    public static final class GatewayCrashed extends FatalForwardingCrash {
        public GatewayCrashed(String message) {
            super(message);
        }
    }

    private final DownstreamChannel delegate;
    private final AtomicInteger crashOnAttempt;

    /**
     * @param crashOnAttempt 第几次投递尝试后崩溃（1 表示首次发送后立刻崩）。
     */
    public CrashBeforeAckChannel(DownstreamChannel delegate, int crashOnAttempt) {
        this.delegate = delegate;
        this.crashOnAttempt = new AtomicInteger(crashOnAttempt);
    }

    @Override
    public DeliveryBatchResult deliver(List<TelemetryPoint> batch) {
        DeliveryBatchResult result = delegate.deliver(batch);
        if (result == null) {
            return null; // 通道 down，不触发崩溃
        }
        if (crashOnAttempt.decrementAndGet() == 0) {
            throw new GatewayCrashed("simulated power loss after downstream accepted batch of "
                    + batch.size() + " but before local ack");
        }
        return result;
    }

    @Override
    public boolean isUp() {
        return delegate.isUp();
    }
}
