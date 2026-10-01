package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DedupGate;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResponse;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.KeyValueNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 本地验证判定可区分：控制面每类输入都得到独立、可查询的 verdict，
 * ACCEPTED / DUPLICATE / LATE / REJECTED(含具体原因) 无需猜测。
 */
class IngestVerdictClassificationTest {

    private static final Logger log = LoggerFactory.getLogger(IngestVerdictClassificationTest.class);

    @TempDir
    Path bufferDir;

    private IngestResponse feed(IngestService svc, String payload) {
        RawFrame frame = new RawFrame("kv", payload);
        IngestResponse resp = IngestResponse.from(svc.ingest(frame));
        log.info("input={} -> verdict={} reason={} detail={}", payload,
                resp.verdict(), resp.reason(), resp.detail());
        return resp;
    }

    private String kv(long seq, double value) {
        return "source=plc-1;point=t;seq=" + seq + ";ts=1758000000000;value=" + value + ";quality=GOOD";
    }

    @Test
    void everyInputMapsToADistinctQueryableVerdict() {
        IngestService svc = new IngestService(
                List.of(new KeyValueNormalizer()), new DedupGate(),
                new FileTelemetryBuffer(bufferDir, 100));

        IngestResponse accepted = feed(svc, kv(2, 20));
        IngestResponse duplicate = feed(svc, kv(2, 20));
        IngestResponse late = feed(svc, kv(1, 10));
        IngestResponse next = feed(svc, kv(3, 30));

        assertThat(accepted.verdict()).isEqualTo("ACCEPTED");
        assertThat(accepted.seq()).isEqualTo(2);
        assertThat(duplicate.verdict()).isEqualTo("DUPLICATE");
        assertThat(late.verdict()).isEqualTo("LATE");
        assertThat(late.lastSeenSeq()).isEqualTo(2);
        assertThat(next.verdict()).isEqualTo("ACCEPTED");

        // 拒绝：三种原因各自独立
        IngestResponse unknownProtocol = IngestResponse.from(
                svc.ingest(new RawFrame("modbus", "x")));
        IngestResponse malformed = feed(svc, "source=plc-1;point=t");
        IngestResponse invalidField = feed(svc, "source=plc-1;point=t;seq=1;ts=1758000000000;value=NaN");

        assertThat(unknownProtocol.verdict()).isEqualTo("REJECTED");
        assertThat(unknownProtocol.reason()).isEqualTo(RejectReason.UNKNOWN_PROTOCOL);
        assertThat(malformed.verdict()).isEqualTo("REJECTED");
        assertThat(malformed.reason()).isEqualTo(RejectReason.MALFORMED_PAYLOAD);
        assertThat(invalidField.verdict()).isEqualTo("REJECTED");
        assertThat(invalidField.reason()).isEqualTo(RejectReason.INVALID_FIELD);

        // 计数按结论分开，可直接查询
        log.info("counters accepted={} duplicates={} late={} unknown={} malformed={} invalid={}",
                svc.acceptedCount(), svc.duplicateCount(), svc.lateCount(),
                svc.rejectedCount(RejectReason.UNKNOWN_PROTOCOL),
                svc.rejectedCount(RejectReason.MALFORMED_PAYLOAD),
                svc.rejectedCount(RejectReason.INVALID_FIELD));
        assertThat(svc.acceptedCount()).isEqualTo(2);
        assertThat(svc.duplicateCount()).isEqualTo(1);
        assertThat(svc.lateCount()).isEqualTo(1);
        assertThat(svc.rejectedCount(RejectReason.UNKNOWN_PROTOCOL)).isEqualTo(1);
        assertThat(svc.rejectedCount(RejectReason.MALFORMED_PAYLOAD)).isEqualTo(1);
        assertThat(svc.rejectedCount(RejectReason.INVALID_FIELD)).isEqualTo(1);
    }
}
