package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 对账工具。
 *
 * 单下游不变式（数字必须来自真实计数，不能用 received+buffered+evicted 反推 admitted，
 * 否则永远平衡、发现不了问题）：
 * <pre>
 *     admitted（真正进入该下游缓存的数据） == uniqueReceived + buffered + evicted
 * </pre>
 * - uniqueReceived 取自下游<strong>持久身份账本</strong>的身份数，进程重启后仍然成立
 *   （通道的进程内 received 列表重启会清零，不能用于跨重启对账）；
 * - 原单出口路径：evicted = buffer.droppedCount（淘汰最旧）；
 * - 双下游共享路径：没有单侧淘汰（evicted=0），满了是“整条拒绝”，计入 rejectedFull。
 *
 * 双下游一致性：比较两侧<strong>持久账本 + 各自在途缓存窗口</strong>按测点展开的身份序列。
 * 这样一侧尚未发完、甚至网关刚重启（通道 received 已清空）时都能对账；全部 drain 后
 * 即退化为两边已接收身份序列的比较。
 */
public final class Reconciliation {

    private Reconciliation() {
    }

    /**
     * 单下游数字平衡（使用真实的准入计数）。
     * @param admitted       真正准入该下游缓存的条数（真实计数，非反推）
     * @param uniqueReceived 去重后真正接收条数（持久身份账本计数，跨重启成立）
     * @param buffered       仍在缓存、尚未确认条数
     * @param evicted        单侧确定性淘汰最旧条数（双下游共享路径恒为 0）
     */
    public static boolean balanced(long admitted, long uniqueReceived, long buffered, long evicted) {
        return admitted == uniqueReceived + buffered + evicted;
    }

    /** 某下游“已真正接收”的去重身份数：以身份账本为准（文件账本跨重启保留）。 */
    public static long uniqueReceived(Downstream d) {
        return ((SimulatedDownstreamChannel) d.channel()).receiver().store().size();
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
     * 入参为每侧有序的“已接收（或已接收+在途）”点列表，按测点分组后逐组比较。
     * @return 不一致的测点描述；为空表示完全一致。
     */
    public static List<String> diffByPoint(List<TelemetryPoint> left, List<TelemetryPoint> right) {
        return diffPointIdentities(groupByPoint(left), groupByPoint(right));
    }

    /**
     * 取某下游“已真正接收（持久账本）+ 仍在缓存（在途窗口）”的身份序列，按测点分组、
     * 组内 seq 升序。账本跨重启保留，因此该视图在重启后也成立。
     */
    public static Map<String, List<PointIdentity>> receivedOrBuffered(Downstream d) {
        IdentityStore store = ((SimulatedDownstreamChannel) d.channel()).receiver().store();
        List<TelemetryPoint> inFlight = d.buffer().peekBatch(Integer.MAX_VALUE);
        Set<String> keys = new TreeSet<>(store.pointKeys());
        for (TelemetryPoint p : inFlight) {
            keys.add(p.key());
        }
        Map<String, List<PointIdentity>> grouped = new TreeMap<>();
        for (String key : keys) {
            List<PointIdentity> ids = new ArrayList<>(store.identitiesForPoint(key));
            for (TelemetryPoint p : inFlight) {
                if (p.key().equals(key)) {
                    PointIdentity id = PointIdentity.of(p);
                    if (!ids.contains(id)) {
                        ids.add(id);
                    }
                }
            }
            ids.sort(Comparator.comparingLong(PointIdentity::seq));
            grouped.put(key, ids);
        }
        return grouped;
    }

    /**
     * 两侧对账（允许一侧尚未 drain、也允许发生在进程重启之后）：
     * 比较两个下游“持久账本 + 在途窗口”的逐测点身份序列，返回差异，空 = 完全一致。
     */
    public static List<String> crossDownstreamDiffs(Downstream reference, Downstream other) {
        return diffPointIdentities(receivedOrBuffered(reference), receivedOrBuffered(other));
    }

    private static List<String> diffPointIdentities(Map<String, List<PointIdentity>> l,
                                                    Map<String, List<PointIdentity>> r) {
        Set<String> keys = new TreeSet<>();
        keys.addAll(l.keySet());
        keys.addAll(r.keySet());
        List<String> diffs = new ArrayList<>();
        for (String key : keys) {
            List<PointIdentity> ls = l.getOrDefault(key, List.of());
            List<PointIdentity> rs = r.getOrDefault(key, List.of());
            if (!ls.equals(rs)) {
                diffs.add(key + ": left(count=" + ls.size() + ")=" + ls
                        + " right(count=" + rs.size() + ")=" + rs);
            }
        }
        return diffs;
    }

    /** 汇总单下游对账数字（全部为真实计数；已接收以持久账本为准，跨重启成立）。 */
    public static Map<String, Long> snapshot(Downstream d) {
        SimulatedDownstreamChannel channel = (SimulatedDownstreamChannel) d.channel();
        TelemetryBuffer buffer = d.buffer();
        long uniqueReceived = uniqueReceived(d);
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("admitted", buffer.appendedCount());
        m.put("receivedUnique", uniqueReceived);
        m.put("buffered", buffer.size());
        m.put("evicted", buffer.droppedCount());
        m.put("rejectedFull", buffer.rejectedCount());
        m.put("duplicates", channel.receiver().duplicateCount());
        m.put("lateAccepted", channel.receiver().lateAcceptedCount());
        m.put("flushedTotal", d.forwarder().flushedTotal());
        m.put("balanced",
                balanced(buffer.appendedCount(), uniqueReceived,
                        buffer.size(), buffer.droppedCount()) ? 1L : 0L);
        return m;
    }
}
