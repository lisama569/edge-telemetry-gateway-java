package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.SharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 采集侧扇出。
 *
 * 两种装配路径：
 * - <b>双下游（默认，强一致）</b>：所有下游共享一条 {@link SharedFanOutBuffer} 准入日志，
 *   append 是“整条原子”的：要么对所有下游一起准入（同一条全局日志、同一顺序），
 *   要么容量满时整条拒绝、所有下游都不收。任何单侧故障/变慢都不会造成
 *   “同一测点序号一边有、一边没有”；慢侧只影响共享容量的腾挪，不阻塞快侧
 *   读取它已可拿到的数据。
 * - <b>原单出口（兼容）</b>：仅向一个独立 {@link TelemetryBuffer} 追加，保留旧的
 *   “容量满淘汰最旧”语义。
 */
public class DownstreamFanOut {

    private static final Logger log = LoggerFactory.getLogger(DownstreamFanOut.class);

    private final SharedFanOutBuffer shared;
    private final List<TelemetryBuffer> buffers;
    private final List<String> names;

    /** 双下游路径：共享准入日志，整条数据原子准入/拒绝。 */
    public DownstreamFanOut(SharedFanOutBuffer shared, List<String> names) {
        this.shared = shared;
        this.names = List.copyOf(names);
        List<TelemetryBuffer> views = new ArrayList<>(names.size());
        for (String name : names) {
            views.add(shared.view(name));
        }
        this.buffers = List.copyOf(views);
    }

    /** 仅扇出到单个缓存（兼容原单出口构造路径）。 */
    public DownstreamFanOut(TelemetryBuffer buffer) {
        this.shared = null;
        this.buffers = List.of(buffer);
        this.names = List.of("cloud");
    }

    /**
     * 扇出一条已采集数据。
     * @return 准入结果；共享路径下 REJECTED_FULL 表示两个下游一致地都没收这条。
     */
    public TelemetryBuffer.AppendOutcome append(TelemetryPoint point) {
        if (shared != null) {
            TelemetryBuffer.AppendOutcome outcome = shared.append(point);
            if (outcome.status() == TelemetryBuffer.AppendStatus.REJECTED_FULL) {
                log.warn("fan-out rejected for ALL {} downstreams (shared buffer full): "
                                + "key={} seq={} rejectedTotal={}",
                        names.size(), point.key(), point.seq(), shared.rejectedCount());
            }
            return outcome;
        }
        // 兼容原单出口：独立缓存，容量满走缓存自身的确定性淘汰路径
        TelemetryBuffer.AppendOutcome outcome = buffers.get(0).append(point);
        if (outcome.status() == TelemetryBuffer.AppendStatus.APPENDED_EVICTED_OLDEST) {
            log.warn("downstream[{}] buffer full, evicted oldest {} while appending {}/{} seq={}",
                    names.get(0), outcome.evicted().key(), outcome.evicted().seq(),
                    point.key(), point.seq());
        }
        return outcome;
    }

    public List<String> names() {
        return names;
    }

    /** 双下游路径因共享容量满被整条拒绝的条数（两侧一致不收）。 */
    public long rejectedOverflowCount() {
        return shared == null ? 0L : shared.rejectedCount();
    }
}
