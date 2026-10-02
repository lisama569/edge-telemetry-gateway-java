package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 对账工具。
 *
 * 单下游不变式（基于真实计数，不能用 received+buffered 反算 appended，否则永远“平衡”）：
 *   已发出(appended) == 真正接收(uniqueReceived) + 仍在途(pending)
 * 共享扇出缓存不淘汰数据，因此没有“静默淘汰”项；若观测到 dropped 不为零（兼容单出口缓存），
 * 则 appended == received + pending + dropped。
 *
 * 双下游一致性：
 * - 两边最终全部确认（在途为 0）后，按测点展开的身份序列（顺序 + 条数）必须完全一致，
 *   否则 {@link #diffByPoint} 必须如实列出差异，绝不允许在分叉时仍报“平衡/正常”；
 * - 允许一边暂时滞后（在途 > 0）：那是“追赶中”，不是不一致；
 *   {@link #reconcilePair} 会明确区分 IN_FLIGHT（正常追赶）与 DIVERGED（真不一致）。
 */
public final class Reconciliation {

    private Reconciliation() {
    }

    /**
     * 单下游平衡：真实已发出 == 真正接收 + 在途 + 淘汰。
     * appended 必须来自计数器（共享缓存 appendedTotal / 采集 accepted），不能由右侧反算。
     */
    public static boolean balanced(long appended, long uniqueReceived, long inFlight, long dropped) {
        return appended == uniqueReceived + inFlight + dropped;
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
     * 双下游逐测点差异：两边按测点的身份序列（条数与顺序）比较。
     * @return 不一致的测点描述；为空表示按已接收视角完全一致。
     */
    public static List<String> diffByPoint(List<TelemetryPoint> left, List<TelemetryPoint> right) {
        Map<String, List<PointIdentity>> l = groupByPoint(left);
        Map<String, List<PointIdentity>> r = groupByPoint(right);
        Set<String> keys = new TreeSet<>();
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

    /** 双下游对账结论。 */
    public enum PairStatus {
        /** 两边在途均为 0 且逐测点序列一致：最终一致。 */
        CONSISTENT,
        /** 至少一边仍有在途数据：正常追赶中（暂未对齐，但尚未构成不一致）。 */
        IN_FLIGHT,
        /** 两边在途均已清空却仍有逐测点差异：真正不一致，必须暴露。 */
        DIVERGED
    }

    /**
     * 基于“真实在途条数 + 已接收序列”的双下游对账。
     *
     * @param leftPending  左下游仍在途（未确认）条数
     * @param rightPending 右下游仍在途条数
     * @param diffs        {@link #diffByPoint} 的结果
     */
    public static PairStatus reconcilePair(long leftPending, long rightPending, List<String> diffs) {
        boolean draining = leftPending > 0 || rightPending > 0;
        if (diffs.isEmpty()) {
            return draining ? PairStatus.IN_FLIGHT : PairStatus.CONSISTENT;
        }
        // 有差异：若仍在途，属于追赶中（差异可能稍后补齐）；两边都清空后仍有差异即为永久分叉。
        return draining ? PairStatus.IN_FLIGHT : PairStatus.DIVERGED;
    }

    /** 汇总单下游对账数字，便于 metrics/日志输出（兼容旧模拟通道）。 */
    public static Map<String, Long> snapshot(Downstream d) {
        SimulatedDownstreamChannel channel = (SimulatedDownstreamChannel) d.channel();
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("receivedUnique", (long) channel.received().size());
        m.put("pending", d.pending());
        if (d.buffer() != null) {
            m.put("dropped", d.buffer().droppedCount());
        }
        m.put("duplicates", channel.receiver().duplicateCount());
        m.put("lateAccepted", channel.receiver().lateAcceptedCount());
        m.put("flushedTotal", d.forwarder().flushedTotal());
        return m;
    }
}
