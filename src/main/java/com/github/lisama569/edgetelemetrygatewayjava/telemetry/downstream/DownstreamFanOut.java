package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 采集侧扇出：同一份数据按拓扑固定顺序追加到每个下游的独立缓存。
 *
 * - 各下游缓存相互独立：某个下游缓存溢出（确定性淘汰最旧）只影响该下游自身对账，
 *   不影响另一个下游收到的条数与顺序；
 * - 同一点先入 cloud 再入 archive，两边入队顺序一致，逐测点序号序列相同；
 * - 不存在“扇出失败”：追加是纯本地操作，容量满走缓存自身的确定性溢出路径。
 */
public class DownstreamFanOut {

    private static final Logger log = LoggerFactory.getLogger(DownstreamFanOut.class);

    private final List<TelemetryBuffer> buffers;
    private final List<String> names;

    public DownstreamFanOut(DownstreamTopology topology) {
        this.buffers = new ArrayList<>(topology.all().size());
        this.names = new ArrayList<>(topology.all().size());
        for (Downstream d : topology.all()) {
            buffers.add(d.buffer());
            names.add(d.name());
        }
    }

    /** 仅扇出到单个缓存（兼容原单出口构造路径）。 */
    public DownstreamFanOut(TelemetryBuffer buffer) {
        this.buffers = List.of(buffer);
        this.names = List.of("cloud");
    }

    /** 同一份数据追加到所有下游缓存；各缓存独立决定是否淘汰最旧。 */
    public void append(TelemetryPoint point) {
        for (int i = 0; i < buffers.size(); i++) {
            TelemetryBuffer.AppendOutcome outcome = buffers.get(i).append(point);
            if (outcome.status() == TelemetryBuffer.AppendStatus.APPENDED_EVICTED_OLDEST) {
                log.warn("downstream[{}] buffer full, evicted oldest {} while appending {}/{} seq={}",
                        names.get(i), outcome.evicted().key(), outcome.evicted().seq(),
                        point.key(), point.seq());
            }
        }
    }

    public List<String> names() {
        return names;
    }
}
