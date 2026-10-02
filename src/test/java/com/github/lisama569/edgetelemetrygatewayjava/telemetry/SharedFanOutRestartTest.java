package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.BufferEndpoint;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.fanout.FileSharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.FileIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Reconciliation;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 共享扇出队列的进程重启语义：
 * - 重启后队列与各下游独立确认游标自动恢复：已确认不重发，未确认按各自进度补齐；
 * - 崩溃在“下游已接收、ack 前”时游标不前进，重启重投被下游文件身份账本判 DUPLICATE；
 * - 全部补齐后两边逐测点序号序列完全一致。
 */
class SharedFanOutRestartTest {

    private static final Logger log = LoggerFactory.getLogger(SharedFanOutRestartTest.class);

    @TempDir
    Path tempDir;

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    private void drain(Forwarder fwd) {
        int n;
        do {
            n = fwd.flushOnce();
        } while (n > 0);
    }

    @Test
    void restartRecoversPerConsumerCursorsAndResumesBothSides() {
        Path dir = tempDir.resolve("fanout");
        Path cloudIds = tempDir.resolve("cloud-ids.jsonl");
        Path archiveIds = tempDir.resolve("archive-ids.jsonl");

        // —— “旧进程”：cloud 批次=2 已确认 seq1,2；archive 停摆，6 条都未确认 ——
        FileSharedFanOutBuffer before = new FileSharedFanOutBuffer(dir, 100);
        BufferEndpoint c0 = before.subscribe("cloud");
        BufferEndpoint a0 = before.subscribe("archive");
        CloudDownstreamChannel cloud0 = new CloudDownstreamChannel("cloud",
                new IdempotentReceiver(new FileIdentityStore(cloudIds)));
        ArchiveDownstreamChannel archive0 = new ArchiveDownstreamChannel("archive",
                new IdempotentReceiver(new FileIdentityStore(archiveIds)));
        Forwarder cloudFwd0 = new Forwarder("cloud", c0, cloud0, 2, 1000);
        Forwarder archiveFwd0 = new Forwarder("archive", a0, archive0, 100, 1000);
        archive0.bringDown();
        for (long s = 1; s <= 6; s++) {
            before.append(point(s));
        }
        assertThat(cloudFwd0.flushOnce()).isEqualTo(2); // cloud 确认 1,2
        assertThat(archiveFwd0.flushOnce()).isZero();
        assertThat(c0.pending()).isEqualTo(4);
        assertThat(a0.pending()).isEqualTo(6);
        assertThat(before.size()).isEqualTo(6); // archive 0 确认，队首无法裁剪
        log.info("before restart: cloud acked=2, archive acked=0, sharedPending=6");

        // —— 模拟进程重启：同一目录重建共享缓存、游标与下游（文件身份账本仍在）——
        FileSharedFanOutBuffer after = new FileSharedFanOutBuffer(dir, 100);
        BufferEndpoint c1 = after.subscribe("cloud");
        BufferEndpoint a1 = after.subscribe("archive");
        assertThat(c1.ackedTotal()).isEqualTo(2);
        assertThat(a1.ackedTotal()).isZero();
        assertThat(after.appendedTotal()).isEqualTo(6);
        assertThat(after.size()).isEqualTo(6);

        CloudDownstreamChannel cloud1 = new CloudDownstreamChannel("cloud",
                new IdempotentReceiver(new FileIdentityStore(cloudIds)));
        ArchiveDownstreamChannel archive1 = new ArchiveDownstreamChannel("archive",
                new IdempotentReceiver(new FileIdentityStore(archiveIds)));
        Forwarder cloudFwd1 = new Forwarder("cloud", c1, cloud1, 100, 1000);
        Forwarder archiveFwd1 = new Forwarder("archive", a1, archive1, 100, 1000);

        // cloud 只续传 3..6（1,2 已确认不重发）；archive 从 1 开始补全部 6 条
        drain(cloudFwd1);
        archive1.bringUp();
        drain(archiveFwd1);

        assertThat(c1.pending()).isZero();
        assertThat(a1.pending()).isZero();
        assertThat(after.size()).isZero();
        // 下游侧“真正接收”合并跨进程视角：cloud 1,2（旧）+3,4,5,6（新），archive 1..6
        assertThat(cloud0.received()).extracting(TelemetryPoint::seq).containsExactly(1L, 2L);
        assertThat(cloud1.received()).extracting(TelemetryPoint::seq).containsExactly(3L, 4L, 5L, 6L);
        assertThat(archive1.received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(cloud1.receiver().duplicateCount()).isZero();
        assertThat(archive1.receiver().duplicateCount()).isZero();

        // 以各自文件身份账本作为跨进程最终事实：两边都恰好持有 1..6
        IdempotentReceiver cloudLedger = new IdempotentReceiver(new FileIdentityStore(cloudIds));
        IdempotentReceiver archiveLedger = new IdempotentReceiver(new FileIdentityStore(archiveIds));
        for (long s = 1; s <= 6; s++) {
            assertThat(cloudLedger.contains(point(s))).as("cloud ledger contains seq %d", s).isTrue();
            assertThat(archiveLedger.contains(point(s))).as("archive ledger contains seq %d", s).isTrue();
        }
        log.info("after restart resume: both ledges hold seq 1..6 exactly once");
    }

    @Test
    void crashAfterDeliveryBeforeAckRedeliversAndDownstreamDedups() {
        Path dir = tempDir.resolve("fanout-crash");
        Path cloudIds = tempDir.resolve("cloud-ids-crash.jsonl");
        Path archiveIds = tempDir.resolve("archive-ids-crash.jsonl");

        FileSharedFanOutBuffer before = new FileSharedFanOutBuffer(dir, 100);
        BufferEndpoint c0 = before.subscribe("cloud");
        BufferEndpoint a0 = before.subscribe("archive");
        CloudDownstreamChannel cloud0 = new CloudDownstreamChannel("cloud",
                new IdempotentReceiver(new FileIdentityStore(cloudIds)));
        ArchiveDownstreamChannel archive0 = new ArchiveDownstreamChannel("archive",
                new IdempotentReceiver(new FileIdentityStore(archiveIds)));
        for (long s = 1; s <= 3; s++) {
            before.append(point(s));
        }
        // cloud 首次投递后、ack 前“进程崩溃”（游标不前进）
        Forwarder crashing = new Forwarder("cloud", c0,
                new CrashBeforeAckChannel(cloud0, 1), 100, 1000);
        try {
            crashing.flushOnce();
            assertThat(false).as("expected simulated crash").isTrue();
        } catch (RuntimeException expected) {
            log.info("simulated crash after cloud delivery, cloud acked={}", c0.ackedTotal());
        }
        assertThat(c0.pending()).isEqualTo(3);
        assertThat(cloud0.received()).hasSize(3);

        // 重启：游标仍是 0，cloud 重投整批 -> 全部 DUPLICATE，archive 正常收 1..3
        FileSharedFanOutBuffer after = new FileSharedFanOutBuffer(dir, 100);
        BufferEndpoint c1 = after.subscribe("cloud");
        BufferEndpoint a1 = after.subscribe("archive");
        CloudDownstreamChannel cloud1 = new CloudDownstreamChannel("cloud",
                new IdempotentReceiver(new FileIdentityStore(cloudIds)));
        ArchiveDownstreamChannel archive1 = new ArchiveDownstreamChannel("archive",
                new IdempotentReceiver(new FileIdentityStore(archiveIds)));
        Forwarder cloudFwd1 = new Forwarder("cloud", c1, cloud1, 100, 1000);
        Forwarder archiveFwd1 = new Forwarder("archive", a1, archive1, 100, 1000);

        assertThat(cloudFwd1.flushOnce()).isEqualTo(3); // 重投并被 ack
        drain(archiveFwd1);
        assertThat(cloud1.receiver().duplicateCount()).isEqualTo(3);
        assertThat(cloud1.received()).isEmpty(); // 没有任何新身份被真正接收
        assertThat(cloud0.received()).extracting(TelemetryPoint::seq).containsExactly(1L, 2L, 3L);
        assertThat(archive1.received()).extracting(TelemetryPoint::seq).containsExactly(1L, 2L, 3L);
        assertThat(Reconciliation.diffByPoint(cloud0.received(), archive1.received())).isEmpty();
        assertThat(after.size()).isZero();
        log.info("crash-before-ack restart: cloud redelivery deduped, both sides seq 1..3 exactly once");
    }
}
