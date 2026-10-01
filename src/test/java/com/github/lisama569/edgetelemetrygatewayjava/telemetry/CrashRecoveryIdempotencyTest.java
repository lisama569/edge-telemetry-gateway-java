package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.CloudDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.FileIdentityStore;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.IdempotentReceiver;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 崩溃续传下的不重复投递：数据已到达下游并登记身份，但本地尚未 ack 就崩溃，
 * 重启续传时下游按身份判 DUPLICATE，同一身份只真正接收一次。
 */
class CrashRecoveryIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryIdempotencyTest.class);

    @TempDir
    Path tempDir;

    private TelemetryPoint point(long seq) {
        return new TelemetryPoint("plc-1", "temperature", seq,
                Instant.ofEpochMilli(1758000000000L), seq * 1.0, TelemetryPoint.Quality.GOOD);
    }

    @Test
    void crashAfterDeliveryBeforeAckDoesNotDoubleReceiveOnRestart() {
        Path bufferFile = tempDir.resolve("cloud-buffer.jsonl");
        Path identityFile = tempDir.resolve("cloud-identities.jsonl");

        // 崩溃前的“旧进程”：文件缓存 + 文件身份账本
        FileTelemetryBuffer buffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiverBefore = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud", receiverBefore);
        for (long seq = 1; seq <= 3; seq++) {
            buffer.append(point(seq));
        }
        log.info("persisted {} points before crash, bufferFile={}", 3, bufferFile);

        Forwarder crashing = new Forwarder("cloud", buffer,
                new CrashBeforeAckChannel(channel, 1), 100, 1000);

        // 首次发送：下游真正接收并已持久化身份，但 ack 前崩溃 -> 异常冒出，缓存未确认
        assertThatThrownBy(crashing::flushOnce)
                .isInstanceOf(CrashBeforeAckChannel.GatewayCrashed.class);
        assertThat(buffer.size()).isEqualTo(3);
        log.info("gateway crashed after delivery; receiver acceptedUnique={} bufferedStill={}",
                receiverBefore.acceptedCount(), buffer.size());

        // 模拟进程重启：从同一文件恢复缓存；身份账本文件仍在（下游侧幂等记忆）
        FileTelemetryBuffer restartedBuffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiverAfter = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel restartedChannelInstance = new CloudDownstreamChannel("cloud", receiverAfter);
        Forwarder restarted = new Forwarder("cloud", restartedBuffer, restartedChannelInstance, 100, 1000);

        assertThat(restartedBuffer.size()).isEqualTo(3); // 未确认数据全部恢复
        int confirmed = restarted.flushOnce();

        log.info("after restart: confirmed={} duplicatesSuppressed={} acceptedByDownstream={}",
                confirmed, receiverAfter.duplicateCount(), channel.received().size());

        // 网关侧：本批确实重投并被 ack（续传完成）
        assertThat(confirmed).isEqualTo(3);
        assertThat(restartedBuffer.size()).isZero();
        // 下游侧：重启后的 3 条全部判 DUPLICATE
        assertThat(receiverAfter.duplicateCount()).isEqualTo(3);
        assertThat(receiverAfter.acceptedCount()).isZero();
        // 同一身份只真正接收一次：崩溃前接收的 3 条仍在，没有第 4~6 条
        assertThat(channel.received()).hasSize(3);
        assertThat(channel.received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void crashMidBatchAcksNothingAndStillDeliversExactlyOnce() {
        Path bufferFile = tempDir.resolve("b2.jsonl");
        Path identityFile = tempDir.resolve("i2.jsonl");
        FileTelemetryBuffer buffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiver = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud", receiver);
        for (long seq = 1; seq <= 4; seq++) {
            buffer.append(point(seq));
        }

        // 崩溃点在第一次发送后（整批 4 条已到下游），ack=0
        Forwarder crashing = new Forwarder("cloud", buffer,
                new CrashBeforeAckChannel(channel, 1), 4, 1000);
        assertThatThrownBy(crashing::flushOnce).isInstanceOf(CrashBeforeAckChannel.GatewayCrashed.class);
        assertThat(buffer.size()).isEqualTo(4);
        log.info("mid-batch crash: receiver.accepted={} buffered={}", receiver.acceptedCount(), buffer.size());

        FileTelemetryBuffer restartedBuffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiver2 = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel channel2 = new CloudDownstreamChannel("cloud", receiver2);
        Forwarder restarted = new Forwarder("cloud", restartedBuffer, channel2, 4, 1000);
        int confirmed = restarted.flushOnce();

        assertThat(confirmed).isEqualTo(4);
        assertThat(channel.received()).extracting(TelemetryPoint::seq)
                .containsExactly(1L, 2L, 3L, 4L);
        assertThat(receiver2.duplicateCount()).isEqualTo(4);
        // 再触发一次 flush：空队，不重发
        assertThat(restarted.flushOnce()).isZero();
        assertThat(channel.received()).hasSize(4);
        log.info("repeated flush after restart: received still {} (no re-delivery)", channel.received().size());
    }

    @Test
    void newDataAfterRestartIsNotMistakenAsDuplicate() {
        Path bufferFile = tempDir.resolve("b3.jsonl");
        Path identityFile = tempDir.resolve("i3.jsonl");
        FileTelemetryBuffer buffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiver = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel channel = new CloudDownstreamChannel("cloud", receiver);
        for (long seq = 1; seq <= 2; seq++) {
            buffer.append(point(seq));
        }
        assertThatThrownBy(() ->
                new Forwarder("cloud", buffer, new CrashBeforeAckChannel(channel, 1), 100, 1000)
                        .flushOnce())
                .isInstanceOf(CrashBeforeAckChannel.GatewayCrashed.class);

        // 重启续传旧数据（被判重），随后写入全新 seq=3 -> 必须正常 ACCEPTED
        FileTelemetryBuffer restartedBuffer = FileTelemetryBuffer.at(bufferFile, 100);
        IdempotentReceiver receiver2 = new IdempotentReceiver(new FileIdentityStore(identityFile));
        CloudDownstreamChannel channel2 = new CloudDownstreamChannel("cloud", receiver2);
        Forwarder restarted = new Forwarder("cloud", restartedBuffer, channel2, 100, 1000);
        assertThat(restarted.flushOnce()).isEqualTo(2);
        assertThat(receiver2.duplicateCount()).isEqualTo(2);

        restartedBuffer.append(point(3));
        assertThat(restarted.flushOnce()).isEqualTo(1);
        assertThat(receiver2.acceptedCount()).isEqualTo(1);
        assertThat(channel2.received()).extracting(TelemetryPoint::seq).containsExactly(3L);

        // 跨重启的最终事实来源是下游身份账本：1/2/3 各在且仅在一次，无重号无缺失
        IdempotentReceiver ledgerView = new IdempotentReceiver(new FileIdentityStore(identityFile));
        assertThat(ledgerView.contains(point(1))).isTrue();
        assertThat(ledgerView.contains(point(2))).isTrue();
        assertThat(ledgerView.contains(point(3))).isTrue();
        assertThat(ledgerView.contains(point(4))).isFalse();
        log.info("new data after restart accepted; ledger contains seq 1,2,3 exactly once");
    }
}
