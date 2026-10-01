package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DeliveryBatchResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.InMemoryIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.List;

/**
 * 本地模拟云端通道（原单出口）：
 * - 可通过 bringUp()/bringDown() 模拟通道抖动与恢复；
 * - 通道 down 时 send 返回 false，调用方不得 ack；
 * - 内置按数据身份去重的幂等接收方：崩溃续传的同一身份不会被真正接收第二次；
 * - 真正接收的去重后数据记录在 received() 中，供测试与本地验证核对；
 * - 可配置模拟时延，用于背压场景。
 *
 * 无参构造使用进程内身份账本（单进程验证用）；需要模拟崩溃续传时注入持久化账本。
 */
public class SimulatedCloudChannel extends CloudDownstreamChannel implements CloudChannel {

    public SimulatedCloudChannel() {
        this(new IdempotentReceiver(new InMemoryIdentityStore()));
    }

    public SimulatedCloudChannel(IdempotentReceiver receiver) {
        super("cloud", receiver);
    }

    @Override
    public boolean send(List<TelemetryPoint> batch) {
        DeliveryBatchResult result = super.deliver(batch);
        return result != null;
    }
}
