package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 归档旁路（云端 + 本地归档）双下游一致性对账：
 * - 两边各自独立消费，收到的测点集合、顺序、条数必须对得上；
 * - 一个下游变慢/失败不拖住另一个，也不导致另一个多收/漏收；
 * - 通道恢复后各自按自己的进度续传，重复 flush/重启不重发已确认数据；
 * - 缓存容量到上限时处置确定（REJECTED_BUFFER_FULL），新下游纳入对账。
 */
class DualDownstreamReconciliationTest {

    private static final Logger log = LoggerFactory.getLogger(DualDownstreamReconciliationTest.class);

    @TempDir
    Path dir;

    private FileTelemetryBuffer buffer;
    private IdempotentSimulatedChannel cloud;
    private IdempotentSimulatedChannel archive;
    private FanOutForwarder forwarder;

    @BeforeEach
    void setUp() {
        buffer = new FileTelemetryBuffer(dir.resolve("buffer"), 100);
        cloud = new IdempotentSimulatedChannel("cloud", dir.resolve("ledger"));
        archive = new IdempotentSimulatedChannel("archive", dir.resolve("ledger"));
        forwarder = new FanOutForwarder(buffer,
                List.of(new Downstream("cloud", cloud), new Downstream("archive", archive)), 10, 1000);
    }

    private List<TelemetryPoint> ingest(int n) {
        List<TelemetryPoint> points = new ArrayList<>();
        for (long seq = 1; seq <= n; seq++) {
            TelemetryPoint p = new TelemetryPoint("plc-1", "temperature", seq,
                    java.time.Instant.ofEpochMilli(1758000000000L + seq), seq * 1.5,
                    TelemetryPoint.Quality.GOOD);
            var outcome = buffer.append(p);
            log.info("append seq={} -> status={} offset={}", seq, outcome.status(), outcome.offset());
            assertThat(outcome.status()).isEqualTo(FileTelemetryBuffer.AppendStatus.APPENDED);
            points.add(p);
        }
        return points;
    }

    private List<Long> seqs(IdempotentSimulatedChannel ch) {
        return ch.received().stream().map(TelemetryPoint::seq).toList();
    }

    private void drain(String name) {
        drainWith(forwarder, name);
    }

    /** flushOnce 返回本批条数；循环条件为"本轮确实发送了一个批次"，直到无待发数据或通道 down。 */
    private void drainWith(FanOutForwarder f, String name) {
        int guard = 0;
        int n;
        do {
            n = f.flushOnce(name);
        } while (n > 0 && guard++ < 1000);
    }

    @Test
    void bothDownstreamsReceiveIdenticalSetOrderAndCount() {
        ingest(7);
        drain("cloud");
        drain("archive");

        log.info("cloud received={}", seqs(cloud));
        log.info("archive received={}", seqs(archive));
        assertThat(seqs(cloud)).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        assertThat(seqs(archive)).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        // 跨下游逐点对账：顺序与条数一致
        assertThat(cloud.received()).usingRecursiveFieldByFieldElementComparator()
                .isEqualTo(archive.received());
        assertThat(buffer.size()).isZero();
    }

    @Test
    void oneDownstreamDownDoesNotBlockTheOtherAndNeverCausesLossOrDuplication() {
        ingest(5);
        archive.bringDown();

        // cloud 正常消费；archive 不动
        drain("cloud");
        assertThat(seqs(cloud)).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(archive.received()).isEmpty();
        // archive down 的轮次被跳过且不推进游标；数据保留
        assertThat(forwarder.flushOnce("archive")).isZero();
        assertThat(forwarder.skippedDown("archive")).isGreaterThan(0);
        assertThat(forwarder.ackedOffset("archive")).isEqualTo(-1L);
        // cloud 不受影响，重复触发不重发
        assertThat(forwarder.flushOnce("cloud")).isZero();
        assertThat(cloud.received()).hasSize(5);
        log.info("archive down: cloud={} archive={} buffered={}", seqs(cloud), seqs(archive), buffer.size());

        // 恢复后 archive 按自己进度续传；慢下游占住数据，cloud 不会多收
        archive.bringUp();
        drain("archive");
        assertThat(seqs(archive)).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(buffer.size()).isZero();
        assertThat(cloud.received()).hasSize(5);
        log.info("after archive recovery: cloud={} archive={}", seqs(cloud), seqs(archive));
    }

    @Test
    void slowDownstreamKeepsDataUntilBothAckAndRepeatedFlushDoesNotDuplicate() {
        ingest(3);
        drain("cloud");
        // cloud 已确认但 archive 尚未确认前，数据不能物理移除
        assertThat(buffer.size()).isEqualTo(3);
        drain("archive");
        assertThat(buffer.size()).isZero();

        // 重复触发：已确认数据不再出现
        assertThat(forwarder.flushOnce("cloud")).isZero();
        assertThat(forwarder.flushOnce("archive")).isZero();
        assertThat(cloud.received()).hasSize(3);
        assertThat(archive.received()).hasSize(3);
    }

    @Test
    void restartResumesEachDownstreamAtItsOwnProgress() {
        ingest(4);
        drain("cloud");

        // 模拟进程重启：buffer、ledger、forwarder 全部用同目录重建，archive 从未确认
        FileTelemetryBuffer restartedBuffer = new FileTelemetryBuffer(dir.resolve("buffer"), 100);
        IdempotentSimulatedChannel restartedCloud =
                new IdempotentSimulatedChannel("cloud", dir.resolve("ledger"));
        IdempotentSimulatedChannel restartedArchive =
                new IdempotentSimulatedChannel("archive", dir.resolve("ledger"));
        FanOutForwarder restarted = new FanOutForwarder(restartedBuffer,
                List.of(new Downstream("cloud", restartedCloud),
                        new Downstream("archive", restartedArchive)), 10, 1000);

        // cloud 游标已确认全部 4 条，重启后无数据可发；未被 archive 确认的数据仍保留
        assertThat(restartedBuffer.size()).isEqualTo(4);
        assertThat(restarted.flushOnce("cloud")).isZero();
        assertThat(restartedCloud.received()).isEmpty();
        assertThat(restarted.ackedOffset("cloud")).isEqualTo(3L);
        assertThat(restarted.ackedOffset("archive")).isEqualTo(-1L);
        log.info("after restart: cloud cursor={} archive cursor={} buffered={}",
                restarted.ackedOffset("cloud"), restarted.ackedOffset("archive"), restartedBuffer.size());

        drainWith(restarted, "archive");
        assertThat(seqs(restartedArchive)).containsExactly(1L, 2L, 3L, 4L);
        assertThat(restartedBuffer.size()).isZero();
        // 两边最终集合对账一致
        assertThat(seqs(restartedArchive)).containsExactlyElementsOf(seqs(cloud));
    }

    @Test
    void bufferCapRejectsDeterministicallyWhenSlowDownstreamOwnsHead() {
        FileTelemetryBuffer smallBuffer = new FileTelemetryBuffer(dir.resolve("small"), 3);
        IdempotentSimulatedChannel smallCloud =
                new IdempotentSimulatedChannel("cloud", dir.resolve("ledger2"));
        IdempotentSimulatedChannel smallArchive =
                new IdempotentSimulatedChannel("archive", dir.resolve("ledger2"));
        FanOutForwarder smallForwarder = new FanOutForwarder(smallBuffer,
                List.of(new Downstream("cloud", smallCloud),
                        new Downstream("archive", smallArchive)), 10, 1000);
        smallArchive.bringDown(); // archive 变慢/不可用

        IntStream.rangeClosed(1, 3).forEach(seq -> {
            TelemetryPoint p = new TelemetryPoint("plc-1", "t", seq,
                    java.time.Instant.ofEpochMilli(1758000000000L + seq), seq, TelemetryPoint.Quality.GOOD);
            assertThat(smallBuffer.append(p).status()).isEqualTo(FileTelemetryBuffer.AppendStatus.APPENDED);
        });
        TelemetryPoint overflow = new TelemetryPoint("plc-1", "t", 4,
                java.time.Instant.ofEpochMilli(1758000000100L), 4, TelemetryPoint.Quality.GOOD);
        var rejected = smallBuffer.append(overflow);
        log.info("overflow append seq=4 -> status={} rejectedFullTotal={}",
                rejected.status(), smallBuffer.rejectedFullCount());

        // 确定性拒绝：不入任何下游，不淘汰队首
        assertThat(rejected.status()).isEqualTo(FileTelemetryBuffer.AppendStatus.REJECTED_BUFFER_FULL);
        assertThat(smallBuffer.rejectedFullCount()).isEqualTo(1);
        drainWith(smallForwarder, "cloud");
        assertThat(seqs(smallCloud)).containsExactly(1L, 2L, 3L);
        // cloud 收完后队首仍被 archive 占用，新写入依旧被拒（两侧集合必须一致）
        assertThat(smallBuffer.append(overflow).status())
                .isEqualTo(FileTelemetryBuffer.AppendStatus.REJECTED_BUFFER_FULL);
        smallArchive.bringUp();
        drainWith(smallForwarder, "archive");
        assertThat(seqs(smallArchive)).containsExactly(1L, 2L, 3L);
        assertThat(smallBuffer.size()).isZero();
        log.info("cap reconciliation: cloud={} archive={} rejectedFull={}",
                seqs(smallCloud), seqs(smallArchive), smallBuffer.rejectedFullCount());
    }
}
