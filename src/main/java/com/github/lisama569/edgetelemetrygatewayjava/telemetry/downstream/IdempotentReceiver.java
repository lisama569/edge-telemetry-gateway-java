package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 下游幂等接收方：按数据自身身份 (sourceId, pointId, seq) 做判定，
 * 保证同一身份只被真正接收一次。
 *
 * 判定规则（与内容无关，仅凭身份）：
 * - 身份已在账本中            -> DUPLICATE：崩溃续传/重投的同一身份，幂等丢弃，不真正接收；
 * - 身份首次出现但 seq < 水位 -> LATE：真实迟到数据，仍真正接收（不被误判为重复），仅标记；
 * - 身份首次出现且 seq >= 水位-> ACCEPTED：正常新数据，真正接收。
 *
 * 账本由 {@link IdentityStore} 承载：文件账本独立于网关进程，
 * 进程在转发中途崩溃、重启后续传时，重复身份仍被识别为 DUPLICATE。
 */
public class IdempotentReceiver {

    private static final Logger log = LoggerFactory.getLogger(IdempotentReceiver.class);

    private final IdentityStore store;
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong lateAccepted = new AtomicLong();

    public IdempotentReceiver(IdentityStore store) {
        this.store = store;
    }

    /**
     * 逐点判定一个批次。返回的结果顺序与输入顺序一一对应。
     * 注意：只有在通道真正可用时才应调用（由通道实现保证）。
     */
    public List<DeliveryResult> deliver(List<TelemetryPoint> batch) {
        List<DeliveryResult> results = new ArrayList<>(batch.size());
        for (TelemetryPoint point : batch) {
            PointIdentity identity = PointIdentity.of(point);
            DeliveryVerdict verdict;
            if (store.contains(identity)) {
                verdict = DeliveryVerdict.DUPLICATE;
                duplicates.incrementAndGet();
                log.info("downstream duplicate suppressed: {}", identity);
            } else {
                long watermark = store.watermark(identity.pointKey());
                if (watermark >= 0 && identity.seq() < watermark) {
                    verdict = DeliveryVerdict.LATE;
                    lateAccepted.incrementAndGet();
                    log.info("downstream late point accepted: {} (watermark={})", identity, watermark);
                } else {
                    verdict = DeliveryVerdict.ACCEPTED;
                    accepted.incrementAndGet();
                }
                // 真实接收：登记身份并推进水位（LATE 也接收，只是不推进超过已有水位）
                store.accept(identity);
            }
            results.add(new DeliveryResult(identity, verdict));
        }
        return results;
    }

    /** 真正接收的新数据条数（不含被幂等丢弃的重复）。 */
    public long acceptedCount() {
        return accepted.get();
    }

    /** 被幂等丢弃的重复投递条数。 */
    public long duplicateCount() {
        return duplicates.get();
    }

    /** 首次出现但落后于水位、仍被真正接收的迟到条数。 */
    public long lateAcceptedCount() {
        return lateAccepted.get();
    }

    public boolean contains(TelemetryPoint point) {
        return store.contains(PointIdentity.of(point));
    }

    /** 下游已接收身份账本（持久账本跨重启保留，用于对账）。 */
    public IdentityStore store() {
        return store;
    }
}
