package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * 崩溃续传下不重复投递的结果保证（本地模拟）：
 * - 数据已被下游成功接收（台账已落盘）但本地尚未确认就断电，重启重放时下游按身份判重，
 *   同一 (sourceId, pointId, seq) 在每个下游最终只真正接收一次；
 * - 转发中途崩溃（批次内部分身份已登记）重启后续传：已登记的判重、未登记的补收，结果恰好一次；
 * - 真正迟到的数据（较小 seq，从未投递过）与新数据（更大 seq）不会被误判成重复而丢掉；
 * - 双下游各自独立幂等，互不影响。
 */
class CrashRecoveryIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryIdempotencyTest.class);

    @TempDir
    Path dir;

    private Path bufferDir;
    private Path ledgerDir;

    @BeforeEach
    void setUp() {
        bufferDir = dir.resolve("buffer");
        ledgerDir = dir.resolve("ledger");
    }

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L + seq), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    private void appendPoints(FileTelemetryBuffer buffer, long from, long to) {
        for (long seq = from; seq <= to; seq++) {
            assertThat(buffer.append(point(seq)).status())
                    .isEqualTo(FileTelemetryBuffer.AppendStatus.APPENDED);
        }
    }

    private FanOutForwarder rebuildForwarder(FileTelemetryBuffer buffer,
                                             IdempotentSimulatedChannel cloud,
                                             IdempotentSimulatedChannel archive) {
        return new FanOutForwarder(buffer,
                List.of(new Downstream("cloud", cloud), new Downstream("archive", archive)), 100, 1000);
    }

    @Test
    void crashAfterDownstreamReceivedButBeforeLocalAckReplaysAsDuplicateExactlyOnce() {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 100);
        IdempotentSimulatedChannel cloud = new IdempotentSimulatedChannel("cloud", ledgerDir);
        IdempotentSimulatedChannel archive = new IdempotentSimulatedChannel("archive", ledgerDir);
        FanOutForwarder forwarder = rebuildForwarder(buffer, cloud, archive);
        appendPoints(buffer, 1, 4);

        // 模拟"下游收到并落了台账、本地还没确认就断电"：第 4 次首次登记时崩溃
        cloud.setCrashAfterLedgeredCount(4);
        Throwable t = catchThrowable(() -> forwarder.flushOnce("cloud"));
        assertThat(t).isInstanceOf(IdempotentSimulatedChannel.SimulatedCrash.class);
        // 崩溃前未推进确认游标：4 条仍在缓存
        assertThat(forwarder.ackedOffset("cloud")).isEqualTo(-1L);
        assertThat(buffer.size()).isEqualTo(4);
        log.info("simulated crash: ledgered but not acked, buffered={}", buffer.size());

        // 进程重启：全部从磁盘恢复
        FileTelemetryBuffer restartedBuffer = new FileTelemetryBuffer(bufferDir, 100);
        IdempotentSimulatedChannel restartedCloud = new IdempotentSimulatedChannel("cloud", ledgerDir);
        IdempotentSimulatedChannel restartedArchive = new IdempotentSimulatedChannel("archive", ledgerDir);
        FanOutForwarder restarted = rebuildForwarder(restartedBuffer, restartedCloud, restartedArchive);

        // 重启后 cursor 仍为 -1，整批重放：4 条全部命中幂等台账判重
        int advanced = restarted.flushOnce("cloud");
        assertThat(advanced).isEqualTo(4);
        assertThat(restarted.duplicateDeliveries("cloud")).isEqualTo(4);
        // 跨重启最终结果：台账恰好 4 个身份，每个只真正接收一次；重放本进程 received 为空（全判重）
        assertThat(restartedCloud.acceptedTotal()).isEqualTo(4);
        assertThat(restartedCloud.received()).isEmpty();
        assertThat(restartedCloud.ledgeredIdentities()).containsExactlyInAnyOrder(
                "plc-1/temperature#1", "plc-1/temperature#2",
                "plc-1/temperature#3", "plc-1/temperature#4");
        log.info("after restart replay: acceptedTotal={} replayedReceived={} duplicates={} ledger={}",
                restartedCloud.acceptedTotal(), restartedCloud.received().size(),
                restarted.duplicateDeliveries("cloud"), restartedCloud.ledgeredIdentities());

        // archive 从未收到过，独立消费，4 条全部真正接收
        assertThat(restarted.flushOnce("archive")).isEqualTo(4);
        assertThat(restartedArchive.acceptedTotal()).isEqualTo(4);
        assertThat(restartedArchive.duplicateTotal()).isZero();
        assertThat(restartedBuffer.size()).isZero();
        log.info("archive independent receive: acceptedTotal={}", restartedArchive.acceptedTotal());
    }

    @Test
    void crashMidBatchReplaysPartialAndEndsExactlyOnce() {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 100);
        IdempotentSimulatedChannel cloud = new IdempotentSimulatedChannel("cloud", ledgerDir);
        IdempotentSimulatedChannel archive = new IdempotentSimulatedChannel("archive", ledgerDir);
        FanOutForwarder forwarder = rebuildForwarder(buffer, cloud, archive);
        appendPoints(buffer, 1, 5);

        // 批次内处理到第 2 个身份后崩溃（offset 0、1 已登记；2、3、4 未登记）
        cloud.setCrashAfterLedgeredCount(2);
        Throwable t = catchThrowable(() -> forwarder.flushOnce("cloud"));
        assertThat(t).isInstanceOf(IdempotentSimulatedChannel.SimulatedCrash.class);
        assertThat(cloud.received()).extracting(TelemetryPoint::seq).containsExactly(1L, 2L);
        assertThat(forwarder.ackedOffset("cloud")).isEqualTo(-1L);

        // 重启续传整批
        FileTelemetryBuffer restartedBuffer = new FileTelemetryBuffer(bufferDir, 100);
        IdempotentSimulatedChannel restartedCloud = new IdempotentSimulatedChannel("cloud", ledgerDir);
        IdempotentSimulatedChannel restartedArchive = new IdempotentSimulatedChannel("archive", ledgerDir);
        FanOutForwarder restarted = rebuildForwarder(restartedBuffer, restartedCloud, restartedArchive);

        IdempotentSimulatedChannel.SendResult result = restarted.channel("cloud")
                .sendBatch(restartedBuffer.peekBatchFor("cloud", 100).stream()
                        .map(FileTelemetryBuffer.Envelope::point).toList());
        // 2 条判重 + 3 条补收，最终 5 条恰好接收一次
        assertThat(result.acceptedCount()).isEqualTo(3);
        assertThat(result.duplicateCount()).isEqualTo(2);
        assertThat(restartedCloud.acceptedTotal()).isEqualTo(5);
        // 重启进程内只新接收了补收的 3、4、5；1、2 在崩溃前进程已接收，重放判重
        assertThat(restartedCloud.received()).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L);
        // 跨重启台账包含全部 5 个身份，无重无漏
        assertThat(restartedCloud.ledgeredIdentities()).containsExactlyInAnyOrder(
                "plc-1/temperature#1", "plc-1/temperature#2", "plc-1/temperature#3",
                "plc-1/temperature#4", "plc-1/temperature#5");
        restarted.flushOnce("cloud"); // 用 forwarder 再走一遍，验证判重不产生第二次接收
        assertThat(restartedCloud.acceptedTotal()).isEqualTo(5);
        log.info("mid-batch crash recovery: acceptedTotal={} duplicatesInReplay={} ledger={}",
                restartedCloud.acceptedTotal(), result.duplicateCount(),
                restartedCloud.ledgeredIdentities());
    }

    @Test
    void genuineLateAndNewDataAreNotMistakenForDuplicates() {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 100);
        IdempotentSimulatedChannel cloud = new IdempotentSimulatedChannel("cloud", ledgerDir);
        IdempotentSimulatedChannel archive = new IdempotentSimulatedChannel("archive", ledgerDir);
        FanOutForwarder forwarder = rebuildForwarder(buffer, cloud, archive);
        appendPoints(buffer, 3, 5); // 先只到 3、4、5
        assertThat(forwarder.flushOnce("cloud")).isEqualTo(3);
        forwarder.flushOnce("archive");
        log.info("delivered seqs 3,4,5 first");

        // 真正迟到的数据 seq=2（该身份从未投递过）：在下游侧不是重复，必须真正接收
        // 构造一个只含 seq=2 的新缓存追加（绕过采集侧乱序闸门，直接验证下游幂等边界）
        TelemetryPoint late = point(2);
        String lateIdentity = late.sourceId() + "/" + late.pointId() + "#2";
        assertThat(cloud.isLedgered(0, lateIdentity)).isFalse();
        IdempotentSimulatedChannel.SendResult lateResult = cloud.sendBatch(List.of(late));
        assertThat(lateResult.acceptedCount()).isEqualTo(1);
        assertThat(lateResult.duplicateCount()).isZero();
        log.info("genuine late seq=2 verdict=ACCEPTED at downstream");

        // 真正的重发 seq=3（内容相同的身份再次到达）：判重，绝不二次接收
        IdempotentSimulatedChannel.SendResult dupResult = cloud.sendBatch(List.of(point(3)));
        assertThat(dupResult.acceptedCount()).isZero();
        assertThat(dupResult.duplicateCount()).isEqualTo(1);

        // 新数据 seq=6：照常接收
        buffer.append(point(6));
        assertThat(forwarder.flushOnce("cloud")).isEqualTo(1);

        assertThat(cloud.acceptedTotal()).isEqualTo(5); // 3,4,5 + late 2 + new 6
        assertThat(cloud.received()).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L, 2L, 6L);
        assertThat(cloud.duplicateTotal()).isEqualTo(1); // 仅真正重发的 seq=3
        log.info("late/new not misjudged: receivedSeqs={} duplicateTotal={}",
                cloud.received().stream().map(TelemetryPoint::seq).toList(), cloud.duplicateTotal());
    }
}
