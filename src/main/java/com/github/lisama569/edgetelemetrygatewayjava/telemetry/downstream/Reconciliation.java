package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 对账工具。
 *
 * 单下游不变式：真正接收（去重后） + 仍在缓存 + 溢出淘汰 == 追加给该下游的身份总数。
 * 双下游一致性：两边全部 drain 后，按测点展开的身份序列（顺序 + 条数）必须完全一致。
 */
public final class Reconciliation {

    private Reconciliation() {
    }

    /** 追加总数（含被淘汰的）：真正接收 + 仍在缓存 + 溢出淘汰。 */
    public static boolean balanced(long appended, long uniqueReceived, long buffered, long dropped) {
        return appended == uniqueReceived + buffered + dropped;
    }

    /** 将一批点按测点分组，保持原顺序提取身份。 */
    public static Map<String, List<PointIdentity>> groupByPoint(List<TelemetryPoint> points) {
        Map<String, List<PointIdentity>> grouped = new TreeMap<>();
        for (TelemetryPoint p : points) {
            grouped.computeIfAbsent(p.key(), k -> new ArrayList<>()).add(PointIdentity.of(p));
        }
        return grouped;
    }

    /**
     * 双下游一致性：两边按测点的身份序列（条数与顺序）必须一致。
     * @return 不一致的测点描述；为空表示完全一致。
     */
    public static List<String> diffByPoint(List<TelemetryPoint> left, List<TelemetryPoint> right) {
        Map<String, List<PointIdentity>> l = groupByPoint(left);
        Map<String, List<PointIdentity>> r = groupByPoint(right);
        Set<String> keys = new java.util.TreeSet<>();
        keys.addAll(l.keySet());
        keys.addAll(r.keySet());
        List<String> diffs = new ArrayList<>();
        for (String key : keys) {
            List<PointIdentity> ls = l.getOrDefault(key, List.of());
            List<PointIdentity> rs = r.getOrDefault(key, List.of());
            if (!ls.equals(rs)) {
                diffs.add(key + ": left=" + ls + " right=" + rs);
            }
        }
        return diffs;
    }

    /** 汇总单下游对账数字，便于 metrics/日志输出。 */
    public static Map<String, Long> snapshot(Downstream d) {
        SimulatedDownstreamChannel channel = (SimulatedDownstreamChannel) d.channel();
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("receivedUnique", (long) channel.received().size());
        m.put("buffered", d.buffer().size());
        m.put("dropped", d.buffer().droppedCount());
        m.put("duplicates", channel.receiver().duplicateCount());
        m.put("lateAccepted", channel.receiver().lateAcceptedCount());
        m.put("flushedTotal", d.forwarder().flushedTotal());
        return m;
    }
}
