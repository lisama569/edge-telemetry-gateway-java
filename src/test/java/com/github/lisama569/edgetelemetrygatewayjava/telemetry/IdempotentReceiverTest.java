package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DeliveryResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DeliveryVerdict;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.InMemoryIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 下游幂等接收方判定测试：
 * - 同一身份崩溃续传/重投只真正接收一次（DUPLICATE 不真正接收）；
 * - 真实迟到数据与全新数据不被误判为重复（LATE 仍真正接收，新数据 ACCEPTED）；
 * - 判定与输入逐条一一对应。
 */
class IdempotentReceiverTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentReceiverTest.class);

    private TelemetryPoint point(String point, long seq, double value) {
        return new TelemetryPoint("plc-1", point, seq,
                Instant.ofEpochMilli(1758000000000L), value, TelemetryPoint.Quality.GOOD);
    }

    private void logResults(String scenario, List<TelemetryPoint> batch, List<DeliveryResult> results) {
        for (int i = 0; i < batch.size(); i++) {
            TelemetryPoint p = batch.get(i);
            log.info("{}: input {}/seq={} -> {}", scenario, p.pointId(), p.seq(),
                    results.get(i).verdict());
        }
    }

    @Test
    void redeliveredIdentityIsAcceptedExactlyOnce() {
        IdempotentReceiver receiver = new IdempotentReceiver(new InMemoryIdentityStore());

        List<TelemetryPoint> first = List.of(point("t", 1, 10), point("t", 2, 20));
        List<DeliveryResult> r1 = receiver.deliver(first);
        logResults("first-delivery", first, r1);
        assertThat(r1).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.ACCEPTED, DeliveryVerdict.ACCEPTED);

        // 同一批次重投（模拟崩溃后续传）：全部判 DUPLICATE，不真正接收
        List<DeliveryResult> r2 = receiver.deliver(first);
        logResults("redelivery", first, r2);
        assertThat(r2).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.DUPLICATE, DeliveryVerdict.DUPLICATE);
        assertThat(receiver.acceptedCount()).isEqualTo(2);
        assertThat(receiver.duplicateCount()).isEqualTo(2);
    }

    @Test
    void genuineLateDataIsAcceptedNotMistakenForDuplicate() {
        IdempotentReceiver receiver = new IdempotentReceiver(new InMemoryIdentityStore());

        // 先收到较大序号 3
        List<TelemetryPoint> head = List.of(point("t", 3, 30));
        List<DeliveryResult> rh = receiver.deliver(head);
        logResults("head", head, rh);
        assertThat(rh).extracting(DeliveryResult::verdict).containsExactly(DeliveryVerdict.ACCEPTED);

        // 真实迟到：身份从未接收过、但 seq 落后水位 -> LATE，仍真正接收，不丢弃
        List<TelemetryPoint> late = List.of(point("t", 1, 10), point("t", 2, 20));
        List<DeliveryResult> rl = receiver.deliver(late);
        logResults("late", late, rl);
        assertThat(rl).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.LATE, DeliveryVerdict.LATE);
        assertThat(receiver.contains(point("t", 1, 10))).isTrue();
        assertThat(receiver.contains(point("t", 2, 20))).isTrue();

        // 全新数据继续 ACCEPTED，不被迟到误判影响
        List<TelemetryPoint> fresh = List.of(point("t", 4, 40));
        List<DeliveryResult> rf = receiver.deliver(fresh);
        logResults("fresh", fresh, rf);
        assertThat(rf).extracting(DeliveryResult::verdict).containsExactly(DeliveryVerdict.ACCEPTED);

        // 迟到数据若再次重投 -> DUPLICATE（身份已接收），而不是再被当成 LATE
        List<DeliveryResult> lateAgain = receiver.deliver(List.of(point("t", 1, 10)));
        logResults("late-redelivery", List.of(point("t", 1, 10)), lateAgain);
        assertThat(lateAgain).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.DUPLICATE);

        assertThat(receiver.acceptedCount()).isEqualTo(2);     // seq 3,4
        assertThat(receiver.lateAcceptedCount()).isEqualTo(2); // seq 1,2（真正接收，仅标记迟到）
        assertThat(receiver.duplicateCount()).isEqualTo(1);
    }

    @Test
    void differentPointsHaveIndependentIdentities() {
        IdempotentReceiver receiver = new IdempotentReceiver(new InMemoryIdentityStore());
        List<TelemetryPoint> batch = List.of(point("a", 1, 1), point("b", 1, 1));
        List<DeliveryResult> results = receiver.deliver(batch);
        logResults("distinct-points", batch, results);
        assertThat(results).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.ACCEPTED, DeliveryVerdict.ACCEPTED);
        // 同样的 seq 但测点不同，不是重复
        List<DeliveryResult> again = receiver.deliver(
                List.of(point("a", 1, 1), point("c", 1, 1)));
        assertThat(again).extracting(DeliveryResult::verdict)
                .containsExactly(DeliveryVerdict.DUPLICATE, DeliveryVerdict.ACCEPTED);
    }

    @Test
    void resultsCorrespondOneToOneToInputOrder() {
        IdempotentReceiver receiver = new IdempotentReceiver(new InMemoryIdentityStore());
        List<TelemetryPoint> batch = List.of(
                point("t", 1, 1), point("t", 1, 1), point("t", 2, 2));
        List<DeliveryResult> results = receiver.deliver(batch);
        logResults("mixed-batch", batch, results);
        // 同批内重复身份也被识别；结果条数与顺序与输入一致
        assertThat(results).hasSize(3);
        assertThat(results.get(0).identity().seq()).isEqualTo(1);
        assertThat(results.get(0).verdict()).isEqualTo(DeliveryVerdict.ACCEPTED);
        assertThat(results.get(1).verdict()).isEqualTo(DeliveryVerdict.DUPLICATE);
        assertThat(results.get(2).verdict()).isEqualTo(DeliveryVerdict.ACCEPTED);
    }
}
