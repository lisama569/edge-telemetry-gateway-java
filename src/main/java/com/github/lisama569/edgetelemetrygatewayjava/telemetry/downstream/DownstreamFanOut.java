package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.FileSharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 采集侧扇出。
 *
 * 双下游（共享扇出）路径：同一份数据只写入一次 {@link FileSharedFanOutBuffer} 共享持久化队列，
 * cloud 与 archive 各自通过独立确认游标（{@link BufferEndpoint}）消费同一条入队序列：
 * - 任意测点任意序号，两个下游最终要么都收到、要么都不收到，条数与顺序一致；
 * - 一个下游停摆/变慢只会让共享队列里属于它的未确认条目堆积，不拖慢另一个下游；
 * - 共享容量满时确定性拒绝本次写入（{@link AppendOutcome#REJECTED_FULL}，背压、可重试），
 *   绝不淘汰任何已入队数据，因此不会出现“一边照收、另一边悄悄丢掉更早数据”。
 *
 * 单出口兼容路径：直接写入一个 {@link TelemetryBuffer}，保持原有“满则确定性淘汰最旧”语义。
 */
public class DownstreamFanOut {

    private static final Logger log = LoggerFactory.getLogger(DownstreamFanOut.class);

    public enum AppendOutcome {
        /** 已入队：该数据对所有下游可见（各自按进度消费）。 */
        APPENDED,
        /** 共享扇出缓存已满：本次写入被确定性拒绝（背压），未入队、未淘汰，调用方应重试。 */
        REJECTED_FULL
    }

    /** 双下游共享路径。 */
    private final FileSharedFanOutBuffer shared;
    /** 单出口兼容路径（非空时 shared 为 null）。 */
    private final TelemetryBuffer single;
    private final String singleName;

    public DownstreamFanOut(FileSharedFanOutBuffer shared) {
        this.shared = shared;
        this.single = null;
        this.singleName = null;
    }

    /** 仅扇出到单个缓存（兼容原单出口构造路径）；保留“满则淘汰最旧”的旧语义。 */
    public DownstreamFanOut(TelemetryBuffer buffer) {
        this.shared = null;
        this.single = buffer;
        this.singleName = "cloud";
    }

    /**
     * 同一份数据进入下游体系。
     * @return APPENDED 已对所有下游可见；REJECTED_FULL 共享缓存满、本次未入队（背压，可重试）。
     */
    public AppendOutcome append(TelemetryPoint point) {
        if (shared != null) {
            FileSharedFanOutBuffer.AppendOutcome outcome = shared.append(point);
            if (outcome.status() == FileSharedFanOutBuffer.AppendStatus.REJECTED_FULL) {
                return AppendOutcome.REJECTED_FULL;
            }
            return AppendOutcome.APPENDED;
        }
        TelemetryBuffer.AppendOutcome outcome = single.append(point);
        if (outcome.status() == TelemetryBuffer.AppendStatus.APPENDED_EVICTED_OLDEST) {
            log.warn("single-egress downstream[{}] buffer full, evicted oldest seq={} key={} while appending seq={}",
                    singleName, outcome.evicted().seq(), outcome.evicted().key(), point.seq());
        }
        return AppendOutcome.APPENDED;
    }

    /** 双下游共享缓存（单出口路径为 null）。 */
    public FileSharedFanOutBuffer sharedBuffer() {
        return shared;
    }
}
