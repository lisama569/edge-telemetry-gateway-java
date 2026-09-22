package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
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
 * 缓存容量溢出：处置行为确定（淘汰最旧 + 计数），顺序保持，重启后可恢复。
 */
class BufferOverflowTest {

    private static final Logger log = LoggerFactory.getLogger(BufferOverflowTest.class);

    @TempDir
    Path bufferDir;

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    @Test
    void overflowEvictsOldestDeterministically() {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 3);

        for (long seq = 1; seq <= 5; seq++) {
            TelemetryBuffer.AppendOutcome outcome = buffer.append(point(seq));
            log.info("append seq={} -> status={} evicted={}", seq, outcome.status(),
                    outcome.evicted() == null ? "none" : outcome.evicted().seq());
        }

        // 容量 3，写入 5 条：seq=1、2 被确定性淘汰
        assertThat(buffer.droppedCount()).isEqualTo(2);
        assertThat(buffer.size()).isEqualTo(3);
        List<TelemetryPoint> remaining = buffer.peekBatch(10);
        assertThat(remaining).extracting(TelemetryPoint::seq).containsExactly(3L, 4L, 5L);
        log.info("after overflow: size={} dropped={} remainingSeqs={}",
                buffer.size(), buffer.droppedCount(),
                remaining.stream().map(TelemetryPoint::seq).toList());
    }

    @Test
    void unackedDataSurvivesRestartAndAckedDataDoesNot() {
        FileTelemetryBuffer buffer = new FileTelemetryBuffer(bufferDir, 10);
        for (long seq = 1; seq <= 5; seq++) {
            buffer.append(point(seq));
        }
        // 确认前 2 条（模拟已成功转发）
        buffer.ack(2);
        log.info("acked 2 of 5, remaining={}", buffer.size());

        // 模拟重启：同一目录重建缓存
        FileTelemetryBuffer restarted = new FileTelemetryBuffer(bufferDir, 10);
        List<TelemetryPoint> recovered = restarted.peekBatch(10);
        log.info("after restart: recoveredSeqs={}",
                recovered.stream().map(TelemetryPoint::seq).toList());

        // 已确认的 1、2 不得重现；未确认的 3、4、5 不得丢失
        assertThat(recovered).extracting(TelemetryPoint::seq).containsExactly(3L, 4L, 5L);
    }
}
