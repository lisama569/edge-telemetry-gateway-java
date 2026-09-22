package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.CsvReadingNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.JsonReadingNormalizer;
import com.github.lisama569.edgetelemetrygatewayjava.normalizer.ProtocolNormalizer.ParseOutcome;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 协议归一拒绝原因可区分性测试。
 */
class NormalizerTest {

    private static final Logger log = LoggerFactory.getLogger(NormalizerTest.class);

    private final JsonReadingNormalizer json = new JsonReadingNormalizer();
    private final CsvReadingNormalizer csv = new CsvReadingNormalizer();

    @Test
    void json_validMessage_normalized() {
        String raw = "{\"sourceId\":\"s1\",\"pointId\":\"temp\",\"seq\":1,"
                + "\"timestampMs\":1727000000000,\"value\":23.5}";
        log.info("[测试输入] JSON 正常报文: {}", raw);
        ParseOutcome outcome = json.parse(raw);
        log.info("[关键判定] accepted={} reading={} reason={}",
                outcome.accepted(), outcome.reading(), outcome.rejectReason());

        assertTrue(outcome.accepted());
        assertEquals("s1", outcome.reading().sourceId());
        assertEquals(1L, outcome.reading().seq());
        assertEquals(23.5, outcome.reading().value(), 1e-9);
    }

    @Test
    void json_rejections_areDistinguishable() {
        String malformed = "not-json";
        String missingField = "{\"sourceId\":\"s1\",\"pointId\":\"temp\",\"seq\":1,"
                + "\"timestampMs\":1727000000000}";
        String badValue = "{\"sourceId\":\"s1\",\"pointId\":\"temp\",\"seq\":\"x\","
                + "\"timestampMs\":1727000000000,\"value\":1}";
        String negativeSeq = "{\"sourceId\":\"s1\",\"pointId\":\"temp\",\"seq\":-1,"
                + "\"timestampMs\":1727000000000,\"value\":1}";

        log.info("[测试输入] 畸形 JSON: {}", malformed);
        assertEquals(RejectReason.MALFORMED, json.parse(malformed).rejectReason());

        log.info("[测试输入] 缺字段 JSON: {}", missingField);
        ParseOutcome missing = json.parse(missingField);
        log.info("[关键判定] reason={} detail={}", missing.rejectReason(), missing.detail());
        assertEquals(RejectReason.MISSING_FIELD, missing.rejectReason());

        log.info("[测试输入] 坏值 JSON: {}", badValue);
        assertEquals(RejectReason.BAD_VALUE, json.parse(badValue).rejectReason());

        log.info("[测试输入] 负序号 JSON: {}", negativeSeq);
        assertEquals(RejectReason.BAD_VALUE, json.parse(negativeSeq).rejectReason());

        assertFalse(json.parse("").accepted());
    }

    @Test
    void csv_validAndInvalid_areDistinguishable() {
        String valid = "sensor-B,pressure,10,1727000000010,1.05";
        log.info("[测试输入] CSV 正常: {}", valid);
        ParseOutcome ok = csv.parse(valid);
        log.info("[关键判定] accepted={} reading={}", ok.accepted(), ok.reading());
        assertTrue(ok.accepted());
        assertEquals(10L, ok.reading().seq());

        String wrongColumns = "sensor-B,pressure,10,1.05";
        log.info("[测试输入] CSV 列数不符: {}", wrongColumns);
        assertEquals(RejectReason.MALFORMED, csv.parse(wrongColumns).rejectReason());

        String nonNumeric = "sensor-B,pressure,abc,1727000000012,1.08";
        log.info("[测试输入] CSV seq 非数字: {}", nonNumeric);
        assertEquals(RejectReason.BAD_VALUE, csv.parse(nonNumeric).rejectReason());

        String emptySource = ",pressure,10,1727000000012,1.08";
        log.info("[测试输入] CSV sourceId 为空: {}", emptySource);
        assertEquals(RejectReason.MISSING_FIELD, csv.parse(emptySource).rejectReason());
    }
}
