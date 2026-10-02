package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Reconciliation;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 对账必须如实反映两边是否一致：
 * - 两边在途为 0 且序列一致 -> CONSISTENT；
 * - 一边还有在途（慢/停恢复后追赶中），即使已接收序列暂时不齐 -> IN_FLIGHT，不能误报 DIVERGED；
 * - 两边在途都为 0 却逐测点对不上（旧缺陷场景）-> DIVERGED，绝不能再报“平衡/正常”；
 * - 单下游平衡恒等式用真实 appended，不能由右侧反算（否则永远平衡）。
 */
class ReconciliationHonestyTest {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationHonestyTest.class);

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    @Test
    void identicalAndFullyAckedIsConsistent() {
        List<TelemetryPoint> both = List.of(point(1), point(2), point(3));
        var diffs = Reconciliation.diffByPoint(both, both);
        Reconciliation.PairStatus status = Reconciliation.reconcilePair(0, 0, diffs);
        assertThat(diffs).isEmpty();
        assertThat(status).isEqualTo(Reconciliation.PairStatus.CONSISTENT);
        log.info("fully acked identical sides -> {}", status);
    }

    @Test
    void laggingSideWithInFlightDataIsInFlightNotDiverged() {
        // cloud 已收到 1..3，archive 只收到 1，但 archive 仍有 2 条在途（追赶中）
        List<TelemetryPoint> cloud = List.of(point(1), point(2), point(3));
        List<TelemetryPoint> archive = List.of(point(1));
        var diffs = Reconciliation.diffByPoint(cloud, archive);
        Reconciliation.PairStatus status = Reconciliation.reconcilePair(0, 2, diffs);
        assertThat(diffs).isNotEmpty();
        assertThat(status).isEqualTo(Reconciliation.PairStatus.IN_FLIGHT);
        log.info("temporally lagging archive with pending=2 -> {}", status);
    }

    @Test
    void fullyAckedMismatchedSidesAreReportedDiverged() {
        // 复现用户的缺陷现象：一边有 seq 1..5，另一边因“静默淘汰”只剩 3..5，且两边在途都已清零。
        List<TelemetryPoint> cloud = List.of(point(1), point(2), point(3), point(4), point(5));
        List<TelemetryPoint> archive = List.of(point(3), point(4), point(5));
        var diffs = Reconciliation.diffByPoint(cloud, archive);
        Reconciliation.PairStatus status = Reconciliation.reconcilePair(0, 0, diffs);
        assertThat(diffs).isNotEmpty();
        assertThat(status).isEqualTo(Reconciliation.PairStatus.DIVERGED);
        assertThat(status.name()).isNotEqualTo("CONSISTENT");
        log.info("fully acked mismatched sides (silent eviction defect) -> {}; diff={}", status, diffs);
    }

    @Test
    void realAppendedCountMustBeUsedForBalance() {
        // 旧实现 appended = received + buffered + dropped 恒等，永远平衡，无法暴露丢数据。
        // 正确口径：真实 appended=5，archive 只收到 3、在途 0、淘汰 2 -> 3+0+2=5 才平衡；
        // 但若没有淘汰计数却少收（received=3, inFlight=0, dropped=0 vs appended=5）-> 不平衡，必须暴露。
        assertThat(Reconciliation.balanced(5, 3, 0, 2)).isTrue();
        assertThat(Reconciliation.balanced(5, 3, 0, 0)).isFalse();
        // 共享扇出语义：不淘汰，appended=5 时 received+inFlight 必须严格等于 5
        assertThat(Reconciliation.balanced(5, 5, 0, 0)).isTrue();
        log.info("balance uses real appended counter: silent mismatch is detectable");
    }
}
