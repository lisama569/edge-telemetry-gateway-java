package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.SharedFanOutBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.ArchiveDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamFanOut;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 双下游共享准入日志的“进程重启续传”：
 * - 重启前 cloud 已确认部分、archive 全欠；重启后两个下游各自按游标把欠下的补齐；
 * - cloud 游标之前已确认的条目不再出现在它的窗口（不重发）；边界批次若被下游再收到，
 *   由文件身份账本判 DUPLICATE，真正接收只发生一次；
 * - 重启后写入的全新序号正常 ACCEPTED，不被误判重复；最终两边逐测点序号序列完全一致。
 */
class SharedFanOutRestartTest {

    private static final Logger log = LoggerFactory.getLogger(SharedFanOutRestartTest.class);

    @TempDir
    Path tempDir;

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    private record Rig(SharedFanOutBuffer shared,
                       DownstreamTopology topology,
                       DownstreamFanOut fanOut,
                       CloudDownstreamChannel cloud,
                       ArchiveDownstreamChannel archive) {
    }

    /** 每次调用模拟一次全新进程：共享日志/游标/身份账本全部从磁盘恢复。 */
    private Rig boot(Path bufferDir, Path identityDir, int capacity, int cloudBatch, int archiveBatch) {
        SharedFanOutBuffer shared = SharedFanOutBuffer.create(
                bufferDir, capacity, List.of("cloud", "archive"));
        TelemetryBuffer cloudView = shared.view("cloud");
        TelemetryBuffer archiveView = shared.view("archive");
        CloudDownstreamChannel cloud = new CloudDownstreamChannel("cloud",
                new IdempotentReceiver(new FileIdentityStore(identityDir.resolve("cloud-ids.jsonl"))));
        ArchiveDownstreamChannel archive = new ArchiveDownstreamChannel("archive",
                new IdempotentReceiver(new FileIdentityStore(identityDir.resolve("archive-ids.jsonl"))));
        Forwarder cloudFwd = new Forwarder("cloud", cloudView, cloud, cloudBatch, 1000);
        Forwarder archiveFwd = new Forwarder("archive", archiveView, archive, archiveBatch, 1000);
        DownstreamTopology topology = new DownstreamTopology(List.of(
                new Downstream("cloud", cloudView, cloud, cloudFwd),
                new Downstream("archive", archiveView, archive, archiveFwd)));
        return new Rig(shared, topology, new DownstreamFanOut(shared, shared.names()),
                cloud, archive);
    }

    private void drain(Downstream d) {
        int n;
        do {
            n = d.forwarder().flushOnce();
        } while (n > 0);
    }

    @Test
    void resumeAfterRestartFromIndependentCursorsAndDedupeRedelivery() {
        log.info("[场景] 重启续传：cloud 已确认 2 条、archive 全欠 -> 重启 -> 各自补齐，重发被幂等");
        Path bufferDir = tempDir.resolve("fanout");
        Path identityDir = tempDir.resolve("ids");

        // ---- 第一次进程：archive down，cloud 批次=2 只确认了 seq=1,2 ----
        Rig first = boot(bufferDir, identityDir, 100, 2, 100);
        first.archive().bringDown();
        for (long seq = 1; seq <= 6; seq++) {
            first.fanOut().append(point(seq));
        }
        assertThat(first.topology().get("cloud").forwarder().flushOnce()).isEqualTo(2);
        assertThat(first.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L);
        assertThat(first.topology().get("cloud").buffer().size()).isEqualTo(4);
        log.info("pre-restart: cloud acked 1,2; archive owes all 6; process dies");

        // ---- 模拟重启：全新对象，共享日志/游标/身份账本从磁盘恢复 ----
        Rig second = boot(bufferDir, identityDir, 100, 2, 100);
        // 恢复后 cloud 的未确认窗口只剩 3..6（已确认的 1,2 不再取出/重发）
        assertThat(second.topology().get("cloud").buffer().peekBatch(100))
                .extracting(TelemetryPoint::seq).containsExactly(3L, 4L, 5L, 6L);
        assertThat(second.topology().get("archive").buffer().size()).isEqualTo(6);
        log.info("post-restart: cloud window=3..6, archive window=1..6");

        drain(second.topology().get("cloud"));
        drain(second.topology().get("archive"));

        // cloud 在旧进程真正接收 1,2（账本已落盘），重启后续收 3..6；archive 首进程收到 1..6。
        // 跨进程的“真正接收身份集合”以文件账本为准：两边账本都含 1..6。
        for (long seq = 1; seq <= 6; seq++) {
            assertThat(second.cloud().receiver().contains(point(seq))).isTrue();
            assertThat(second.archive().receiver().contains(point(seq))).isTrue();
        }
        assertThat(second.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L, 6L);
        assertThat(second.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(second.cloud().receiver().duplicateCount()).isZero();
        assertThat(second.archive().receiver().duplicateCount()).isZero();
        log.info("post-restart drain: both ledgers contain 1..6, no duplicate delivery");

        // 重启后写入的全新合法序号 7..8 必须正常 ACCEPTED，不被误判为重复
        second.fanOut().append(point(7));
        second.fanOut().append(point(8));
        drain(second.topology().get("cloud"));
        drain(second.topology().get("archive"));
        // 新进程内接收到的序列仍是完整有序前缀的后缀（3..8），无重复
        assertThat(second.cloud().received()).extracting(TelemetryPoint::seq)
                .containsExactly(3L, 4L, 5L, 6L, 7L, 8L);
        assertThat(second.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertThat(second.cloud().receiver().duplicateCount()).isZero();
        assertThat(second.archive().receiver().duplicateCount()).isZero();
        // 两边文件账本身份集合都是 1..8（跨进程最终一致性）
        for (long seq = 1; seq <= 8; seq++) {
            assertThat(second.cloud().receiver().contains(point(seq))).isTrue();
            assertThat(second.archive().receiver().contains(point(seq))).isTrue();
        }

        // 再重启一次：全部已确认，共享日志已压缩，两边窗口都应为空，绝不重发
        Rig third = boot(bufferDir, identityDir, 100, 2, 100);
        assertThat(third.topology().get("cloud").buffer().size()).isZero();
        assertThat(third.topology().get("archive").buffer().size()).isZero();
        drain(third.topology().get("cloud"));
        drain(third.topology().get("archive"));
        assertThat(third.cloud().received()).isEmpty();
        assertThat(third.archive().received()).isEmpty();
        log.info("second restart: both windows empty, nothing resent; both ledgers 1..8");

        for (Downstream d : second.topology().all()) {
            log.info("reconciliation[{}]: {}", d.name(), Reconciliation.snapshot(d));
        }
    }

    @Test
    void crashAfterDeliveryBeforeAckSuppressesRedeliveryOnRestart() {
        log.info("[场景] 下游已收到但 ack 前崩溃：重启续传同批 -> DUPLICATE 抑制，真正接收一次");
        Path bufferDir = tempDir.resolve("fanout-crash");
        Path identityDir = tempDir.resolve("ids-crash");

        Rig first = boot(bufferDir, identityDir, 100, 100, 100);
        for (long seq = 1; seq <= 3; seq++) {
            first.fanOut().append(point(seq));
        }
        // 用崩溃夹具包住 cloud 通道：首次投递成功（身份落盘）后、ack 前抛致命异常
        Forwarder crashing = new Forwarder("cloud",
                first.topology().get("cloud").buffer(),
                new CrashBeforeAckChannel(first.cloud(), 1), 100, 1000);
        org.assertj.core.api.Assertions.assertThatThrownBy(crashing::flushOnce)
                .isInstanceOf(CrashBeforeAckChannel.GatewayCrashed.class);
        // 崩溃未 ack：cloud 游标不动，窗口仍是 1..3
        assertThat(first.topology().get("cloud").buffer().size()).isEqualTo(3);
        assertThat(first.cloud().received()).hasSize(3);
        log.info("crashed after cloud accepted 1..3, cursor not advanced");

        // 重启：共享日志与游标恢复，cloud 再投 1..3 -> 全部 DUPLICATE；archive 首次接收
        Rig second = boot(bufferDir, identityDir, 100, 100, 100);
        drain(second.topology().get("cloud"));
        drain(second.topology().get("archive"));

        assertThat(second.cloud().receiver().duplicateCount()).isEqualTo(3);
        assertThat(second.cloud().received()).isEmpty(); // 本轮没有真正接收新身份
        assertThat(second.archive().received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
        assertThat(second.archive().receiver().duplicateCount()).isZero();
        // 两边文件账本身份集合等价：都含 1..3
        for (long seq = 1; seq <= 3; seq++) {
            assertThat(second.cloud().receiver().contains(point(seq))).isTrue();
            assertThat(second.archive().receiver().contains(point(seq))).isTrue();
        }
        log.info("post-crash: cloud duplicates suppressed=3, archive received 1..3; both ledgers 1..3");

        // 游标已推进：重复 flush 不再重发
        assertThat(second.topology().get("cloud").forwarder().flushOnce()).isZero();
        assertThat(second.topology().get("archive").forwarder().flushOnce()).isZero();
        assertThat(second.cloud().receiver().duplicateCount()).isEqualTo(3);
    }
}
