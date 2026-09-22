package com.github.lisama569.edgetelemetrygatewayjava.telemetry.source;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟现场数据源：按测点生成 seq 单调递增的 kv 协议报文。
 * 供本地联调与演示使用；测试可据此构造乱序/重复/迟到报文。
 */
@Component
public class SimulatedSource {

    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    /** 生成某测点的下一条合法 kv 报文。 */
    public RawFrame nextFrame(String sourceId, String pointId, double value) {
        long seq = sequences.computeIfAbsent(sourceId + "/" + pointId, k -> new AtomicLong())
                .incrementAndGet();
        return frame(sourceId, pointId, seq, value);
    }

    /** 按指定 seq 构造报文（用于乱序/重复/迟到场景）。 */
    public RawFrame frame(String sourceId, String pointId, long seq, double value) {
        String payload = "source=" + sourceId
                + ";point=" + pointId
                + ";seq=" + seq
                + ";ts=" + System.currentTimeMillis()
                + ";value=" + value
                + ";quality=GOOD";
        return new RawFrame("kv", payload);
    }
}
