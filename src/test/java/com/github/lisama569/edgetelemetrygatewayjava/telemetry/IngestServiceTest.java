package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.FileTelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DedupGate;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.KeyValueNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.RejectReason;
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
 * 乱序、重复、迟到与非法报文的采集判定测试。
 * 日志打印每条输入报文与判定结果，便于核对判定依据。
 */
class IngestServiceTest {

    private static final Logger log = LoggerFactory.getLogger(IngestServiceTest.class);

    @TempDir
    Path bufferDir;

    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        ingestService = new IngestService(
                List.of(new KeyValueNormalizer()),
                new DedupGate(),
                new FileTelemetryBuffer(bufferDir, 100));
    }

    private IngestResult feed(String payload) {
        RawFrame frame = new RawFrame("kv", payload);
        IngestResult result = ingestService.ingest(frame);
        log.info("input={} -> {}", payload, result);
        return result;
    }

    private String kv(long seq, double value) {
        return "source=plc-1;point=temperature;seq=" + seq
                + ";ts=1758000000000;value=" + value + ";quality=GOOD";
    }

    @Test
    void outOfOrderDuplicateAndLateFramesAreClassified() {
        // 顺序到达 seq=3（先到的较大序号合法，推进水位）
        assertThat(feed(kv(3, 30.0))).isInstanceOf(IngestResult.Accepted.class);
        // 乱序：seq=1 晚于 seq=3 到达 -> Late，不得覆盖
        IngestResult late = feed(kv(1, 10.0));
        assertThat(late).isInstanceOf(IngestResult.Late.class);
        assertThat(((IngestResult.Late) late).lastSeenSeq()).isEqualTo(3);
        // 重复：seq=3 内容一致 -> Duplicate，不重复计数
        assertThat(feed(kv(3, 30.0))).isInstanceOf(IngestResult.Duplicate.class);
        // 冲突：seq=3 但内容不同 -> Late（绝不覆盖已确认数据）
        assertThat(feed(kv(3, 99.9))).isInstanceOf(IngestResult.Late.class);
        // 正常推进：seq=4 -> Accepted
        assertThat(feed(kv(4, 40.0))).isInstanceOf(IngestResult.Accepted.class);

        log.info("counters: accepted={} duplicates={} late={}",
                ingestService.acceptedCount(), ingestService.duplicateCount(), ingestService.lateCount());
        assertThat(ingestService.acceptedCount()).isEqualTo(2);
        assertThat(ingestService.duplicateCount()).isEqualTo(1);
        assertThat(ingestService.lateCount()).isEqualTo(2);
    }

    @Test
    void malformedFramesAreRejectedWithDistinguishableReasons() {
        // 未知协议
        IngestResult unknownProtocol = ingestService.ingest(new RawFrame("modbus-rtu", "whatever"));
        log.info("input protocol=modbus-rtu -> {}", unknownProtocol);
        assertThat(unknownProtocol).isEqualTo(
                new IngestResult.Rejected(RejectReason.UNKNOWN_PROTOCOL,
                        "no normalizer for protocol: modbus-rtu"));

        // 结构非法：缺字段
        assertThat(feed("source=plc-1;point=temperature"))
                .isInstanceOfSatisfying(IngestResult.Rejected.class,
                        r -> assertThat(r.reason()).isEqualTo(RejectReason.MALFORMED_PAYLOAD));
        // 结构非法：seq 非数字
        assertThat(feed("source=plc-1;point=t;seq=abc;ts=1758000000000;value=1.0"))
                .isInstanceOfSatisfying(IngestResult.Rejected.class,
                        r -> assertThat(r.reason()).isEqualTo(RejectReason.MALFORMED_PAYLOAD));
        // 取值非法：value 为 NaN
        assertThat(feed("source=plc-1;point=t;seq=1;ts=1758000000000;value=NaN"))
                .isInstanceOfSatisfying(IngestResult.Rejected.class,
                        r -> assertThat(r.reason()).isEqualTo(RejectReason.INVALID_FIELD));
        // 取值非法：quality 未知
        assertThat(feed("source=plc-1;point=t;seq=1;ts=1758000000000;value=1.0;quality=MAYBE"))
                .isInstanceOfSatisfying(IngestResult.Rejected.class,
                        r -> assertThat(r.reason()).isEqualTo(RejectReason.INVALID_FIELD));

        assertThat(ingestService.rejectedCount(RejectReason.UNKNOWN_PROTOCOL)).isEqualTo(1);
        assertThat(ingestService.rejectedCount(RejectReason.MALFORMED_PAYLOAD)).isEqualTo(2);
        assertThat(ingestService.rejectedCount(RejectReason.INVALID_FIELD)).isEqualTo(2);
    }

    @Test
    void differentPointsHaveIndependentWatermarks() {
        assertThat(feed("source=plc-1;point=a;seq=5;ts=1758000000000;value=1.0"))
                .isInstanceOf(IngestResult.Accepted.class);
        // 另一测点 seq=1 不受 point=a 水位影响
        assertThat(feed("source=plc-1;point=b;seq=1;ts=1758000000000;value=2.0"))
                .isInstanceOf(IngestResult.Accepted.class);
        assertThat(ingestService.acceptedCount()).isEqualTo(2);
    }
}
