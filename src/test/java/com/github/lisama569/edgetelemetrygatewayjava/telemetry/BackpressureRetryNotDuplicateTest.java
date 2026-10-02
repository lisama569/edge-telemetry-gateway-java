package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DedupGate;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 共享缓存写满时的采集侧语义：
 * - 入队失败 -> REJECTED(BUFFER_FULL)，且<strong>不推进去重水位</strong>；
 * - 同一报文退避后重试（此时入队成功）仍是 ACCEPTED，绝不被误判成 DUPLICATE 而永久丢数据。
 */
class BackpressureRetryNotDuplicateTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureRetryNotDuplicateTest.class);

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    @Test
    void rejectedWhenFullThenRetriedIsAcceptedNotDuplicate() {
        DedupGate gate = new DedupGate();
        AtomicBoolean canEnqueue = new AtomicBoolean(false); // 初始模拟缓存满

        // 第一次：入队失败（背压）
        IngestResult first = gate.checkAndEnqueue(point(1), p -> {
            if (canEnqueue.get()) {
                return true;
            }
            return false;
        });
        assertThat(first).isInstanceOfSatisfying(IngestResult.Rejected.class,
                r -> assertThat(r.reason()).isEqualTo(RejectReason.BUFFER_FULL));
        log.info("first attempt while full -> {}", ((IngestResult.Rejected) first).reason());

        // 下游恢复、容量释放：同一 seq=1 重试入队成功 -> 必须是 ACCEPTED 而不是 DUPLICATE
        canEnqueue.set(true);
        IngestResult retry = gate.checkAndEnqueue(point(1), p -> true);
        assertThat(retry).isInstanceOf(IngestResult.Accepted.class);
        log.info("retry after capacity freed -> ACCEPTED (not mistaken as duplicate)");

        // 真正的重复（内容一致的同一已接收序号）仍是 DUPLICATE
        IngestResult dup = gate.checkAndEnqueue(point(1), p -> true);
        assertThat(dup).isInstanceOf(IngestResult.Duplicate.class);
        // 下一个合法新序号正常 ACCEPTED
        assertThat(gate.checkAndEnqueue(point(2), p -> true)).isInstanceOf(IngestResult.Accepted.class);
        log.info("genuine duplicate still DUPLICATE; subsequent new seq ACCEPTED");
    }
}
