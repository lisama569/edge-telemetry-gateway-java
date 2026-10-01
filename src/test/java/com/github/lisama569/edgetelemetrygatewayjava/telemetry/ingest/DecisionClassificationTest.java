package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 本地验证时判定结果必须可单独识别、统计、查询：
 * ACCEPTED / DUPLICATE / LATE / REJECTED(UNKNOWN_PROTOCOL|MALFORMED_PAYLOAD|INVALID_FIELD|BUFFER_FULL)
 * 输入与判定一一对应（decisionId），合法数据不被误判。
 */
class DecisionClassificationTest {

    private static final Logger log = LoggerFactory.getLogger(DecisionClassificationTest.class);

    @TempDir
    Path bufferDir;

    private IngestService ingest;
    private DecisionLog decisions;
    private FileTelemetryBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new FileTelemetryBuffer(bufferDir, 100);
        decisions = new DecisionLog(50);
        ingest = new IngestService(List.of(new KeyValueNormalizer()), new DedupGate(), buffer, decisions);
    }

    private DecisionRecord feed(String payload) {
        ingest.ingest(new RawFrame("kv", payload));
        DecisionRecord rec = decisions.recent(1).getLast();
        log.info("input={} -> decisionId={} verdict={} reason={} detail={}",
                payload, rec.decisionId(), rec.verdict(), rec.reason(), rec.detail());
        return rec;
    }

    private String kv(long seq, double value) {
        return "source=plc-1;point=t;seq=" + seq + ";ts=1758000000000;value=" + value + ";quality=GOOD";
    }

    @Test
    void everyVerdictIsIndividuallyIdentifiableAndQueryable() {
        DecisionRecord accepted = feed(kv(1, 10));
        DecisionRecord duplicate = feed(kv(1, 10));
        DecisionRecord late = feed(kv(1, 99)); // 同 seq 内容冲突 -> LATE
        DecisionRecord late2 = feed(kv(0, 0));  // seq 回退 -> LATE
        DecisionRecord accepted2 = feed(kv(2, 20));

        assertThat(accepted.verdict()).isEqualTo("ACCEPTED");
        assertThat(duplicate.verdict()).isEqualTo("DUPLICATE");
        assertThat(late.verdict()).isEqualTo("LATE");
        assertThat(late2.verdict()).isEqualTo("LATE");
        assertThat(accepted2.verdict()).isEqualTo("ACCEPTED");

        // 每种结论可按 decisionId 单独查询，且输入与判定一一对应
        for (DecisionRecord rec : List.of(accepted, duplicate, late, late2, accepted2)) {
            DecisionRecord fetched = decisions.find(rec.decisionId()).orElseThrow();
            assertThat(fetched).isEqualTo(rec);
            assertThat(fetched.payload()).startsWith("source=plc-1");
        }
        assertThat(decisions.find("d-000001")).isPresent();
        assertThat(decisions.find("d-999999")).isEmpty();

        // 统计互不混淆
        assertThat(ingest.acceptedCount()).isEqualTo(2);
        assertThat(ingest.duplicateCount()).isEqualTo(1);
        assertThat(ingest.lateCount()).isEqualTo(2);

        List<DecisionRecord> recent = decisions.recent(10);
        assertThat(recent).extracting(DecisionRecord::verdict)
                .containsExactly("ACCEPTED", "DUPLICATE", "LATE", "LATE", "ACCEPTED");
        log.info("verdicts in order: {}", recent.stream().map(DecisionRecord::verdict).toList());
    }

    @Test
    void rejectReasonsAreDistinctIncludingBufferFull() {
        // 未知协议
        IngestResult unknown = ingest.ingest(new RawFrame("modbus", "x"));
        DecisionRecord unknownRec = decisions.recent(1).getLast();
        assertThat(unknown).isInstanceOf(IngestResult.Rejected.class);
        assertThat(unknownRec.verdict()).isEqualTo("REJECTED");
        assertThat(unknownRec.reason()).isEqualTo("UNKNOWN_PROTOCOL");

        assertThat(feed("source=s;point=p").reason()).isEqualTo("MALFORMED_PAYLOAD");
        assertThat(feed("source=s;point=p;seq=1;ts=1758000000000;value=NaN").reason())
                .isEqualTo("INVALID_FIELD");

        // BUFFER_FULL：多下游注册、容量占满后的确定性拒绝（独立目录与水位，避免相互污染）
        FileTelemetryBuffer tiny = new FileTelemetryBuffer(bufferDir.resolve("tiny"), 1);
        DecisionLog tinyLog = new DecisionLog(10);
        IngestService tinyIngest =
                new IngestService(List.of(new KeyValueNormalizer()), new DedupGate(), tiny, tinyLog);
        tiny.registerConsumer("cloud");
        tiny.registerConsumer("archive");
        DecisionRecord first = feedInto(tinyIngest, tinyLog, kv(1, 1));
        DecisionRecord full = feedInto(tinyIngest, tinyLog, kv(2, 2));
        assertThat(first.verdict()).isEqualTo("ACCEPTED");
        assertThat(full.verdict()).isEqualTo("REJECTED");
        assertThat(full.reason()).isEqualTo("BUFFER_FULL");
        log.info("buffer full verdict: {} detail={}", full.reason(), full.detail());
        assertThat(tinyIngest.rejectedCount(RejectReason.BUFFER_FULL)).isEqualTo(1);

        // 各拒绝原因独立计数
        assertThat(ingest.rejectedCount(RejectReason.UNKNOWN_PROTOCOL)).isEqualTo(1);
        assertThat(ingest.rejectedCount(RejectReason.MALFORMED_PAYLOAD)).isEqualTo(1);
        assertThat(ingest.rejectedCount(RejectReason.INVALID_FIELD)).isEqualTo(1);
    }

    @Test
    void legitimateDataNeverMisclassified() {
        // 单调递增的合法报文全部 ACCEPTED
        for (long seq = 1; seq <= 10; seq++) {
            assertThat(feed(kv(seq, seq)).verdict()).isEqualTo("ACCEPTED");
        }
        // 不同测点独立判定，互不影响
        assertThat(feed("source=other;point=x;seq=1;ts=1758000000000;value=1.0").verdict())
                .isEqualTo("ACCEPTED");
        assertThat(ingest.acceptedCount()).isEqualTo(11);
        assertThat(ingest.duplicateCount()).isZero();
        assertThat(ingest.lateCount()).isZero();
        log.info("legitimate sequence all accepted: accepted={}", ingest.acceptedCount());
    }

    private DecisionRecord feedInto(IngestService svc, DecisionLog log0, String payload) {
        svc.ingest(new RawFrame("kv", payload));
        return log0.recent(1).getLast();
    }
}
