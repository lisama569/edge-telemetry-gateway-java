package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DedupGate;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.KeyValueNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 通道不可用 -> 本地缓存 -> 恢复续传 的端到端语义：
 * 不丢、不重、顺序可复现；重复 flush 与重启后已确认数据不重发。
 */
class ChannelRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(ChannelRecoveryTest.class);

    @TempDir
    Path bufferDir;

    private SimulatedCloudChannel channel;
    private FileTelemetryBuffer buffer;
    private Forwarder forwarder;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        channel = new SimulatedCloudChannel();
        buffer = new FileTelemetryBuffer(bufferDir, 100);
        forwarder = new Forwarder(buffer, channel, 2, 1000);
        ingestService = new IngestService(List.of(new KeyValueNormalizer()), new DedupGate(), buffer);
    }

    private void ingestSeqs(long from, long to) {
        for (long seq = from; seq <= to; seq++) {
            String payload = "source=plc-1;point=temperature;seq=" + seq
                    + ";ts=1758000000000;value=" + (seq * 1.0);
            log.info("ingest seq={} -> {}", seq, ingestService.ingest(
                    new com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame("kv", payload)));
        }
    }

    private List<Long> receivedSeqs() {
        return channel.received().stream().map(TelemetryPoint::seq).toList();
    }

    @Test
    void dataBufferedWhileDownAndResentInOrderAfterRecovery() {
        channel.bringDown();
        ingestSeqs(1, 5);
        assertThat(buffer.size()).isEqualTo(5);

        // 通道 down：flush 不确认任何数据，且判定可观测
        assertThat(forwarder.flushOnce()).isZero();
        assertThat(forwarder.skippedChannelDown()).isEqualTo(1);
        assertThat(buffer.size()).isEqualTo(5);
        log.info("channel down: buffered={} skippedChannelDown={}", buffer.size(), forwarder.skippedChannelDown());

        // 恢复后续传：batchSize=2，逐轮 drain，顺序保持
        channel.bringUp();
        int total = 0;
        int n;
        while ((n = forwarder.flushOnce()) > 0) {
            log.info("flush round confirmed {} entries, remaining={}", n, buffer.size());
            total += n;
        }
        assertThat(total).isEqualTo(5);
        assertThat(buffer.size()).isZero();
        assertThat(receivedSeqs()).containsExactly(1L, 2L, 3L, 4L, 5L);

        // 重复 flush：已确认数据不得重发
        assertThat(forwarder.flushOnce()).isZero();
        assertThat(channel.received()).hasSize(5);
        log.info("after recovery: received={} flushedTotal={}", receivedSeqs(), forwarder.flushedTotal());
    }

    @Test
    void restartDuringOutageKeepsUnackedData() {
        channel.bringDown();
        ingestSeqs(1, 4);
        assertThat(buffer.size()).isEqualTo(4);

        // 模拟进程重启：同一目录重建 buffer 与 forwarder，通道仍 down
        FileTelemetryBuffer restartedBuffer = new FileTelemetryBuffer(bufferDir, 100);
        Forwarder restartedForwarder = new Forwarder(restartedBuffer, channel, 100, 1000);
        assertThat(restartedBuffer.size()).isEqualTo(4);
        log.info("restarted with {} unacked entries recovered", restartedBuffer.size());

        channel.bringUp();
        int flushed = restartedForwarder.flushOnce();
        assertThat(flushed).isEqualTo(4);
        assertThat(receivedSeqs()).containsExactly(1L, 2L, 3L, 4L);
        assertThat(restartedBuffer.size()).isZero();
    }

    @Test
    void flakyChannelRetriesWithoutLossOrDuplication() {
        ingestSeqs(1, 6);
        // 抖动：第一轮 down，第二轮 up
        channel.bringDown();
        assertThat(forwarder.flushOnce()).isZero();
        channel.bringUp();
        int total = 0;
        int n;
        while ((n = forwarder.flushOnce()) > 0) {
            total += n;
        }
        log.info("flaky channel: flushedTotal={} failedRounds={} received={}",
                total, forwarder.failedRounds(), receivedSeqs());
        assertThat(total).isEqualTo(6);
        assertThat(receivedSeqs()).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
    }
}
