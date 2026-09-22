package com.github.lisama569.edgetelemetrygatewayjava.normalizer;

import com.github.lisama569.edgetelemetrygatewayjava.normalizer.ProtocolNormalizer.ParseOutcome;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * CSV 报文归一化器。
 *
 * 固定 5 列（逗号分隔，首尾空白会被 trim）：
 * <pre>sourceId,pointId,seq,timestampMs,value</pre>
 * 拒绝原因区分：
 * - MALFORMED：空报文或列数不等于 5；
 * - MISSING_FIELD：sourceId / pointId 为空白；
 * - BAD_VALUE：seq/timestampMs/value 不可解析、序号或时间戳为负、value 非有限数。
 */
@Component
public class CsvReadingNormalizer implements ProtocolNormalizer {

    private static final Logger log = LoggerFactory.getLogger(CsvReadingNormalizer.class);

    @Override
    public String protocol() {
        return "csv";
    }

    @Override
    public ParseOutcome parse(String raw) {
        log.info("[normalizer:csv] 输入报文原文: {}", raw);
        if (raw == null || raw.isBlank()) {
            return reject(RejectReason.MALFORMED, "报文为空");
        }
        String[] parts = raw.split(",", -1);
        if (parts.length != 5) {
            return reject(RejectReason.MALFORMED, "列数必须为 5, 实际=" + parts.length);
        }
        String sourceId = parts[0].trim();
        String pointId = parts[1].trim();
        if (sourceId.isEmpty()) {
            return reject(RejectReason.MISSING_FIELD, "sourceId 为空");
        }
        if (pointId.isEmpty()) {
            return reject(RejectReason.MISSING_FIELD, "pointId 为空");
        }

        Long seq = parseLong(parts[2], "seq");
        if (seq == null) {
            return reject(RejectReason.BAD_VALUE, "seq 不可解析: '" + parts[2].trim() + "'");
        }
        Long ts = parseLong(parts[3], "timestampMs");
        if (ts == null) {
            return reject(RejectReason.BAD_VALUE, "timestampMs 不可解析: '" + parts[3].trim() + "'");
        }
        Double value = parseDouble(parts[4]);
        if (value == null) {
            return reject(RejectReason.BAD_VALUE, "value 不可解析: '" + parts[4].trim() + "'");
        }
        if (seq < 0) {
            return reject(RejectReason.BAD_VALUE, "seq 不允许为负: " + seq);
        }
        if (ts < 0) {
            return reject(RejectReason.BAD_VALUE, "timestampMs 不允许为负: " + ts);
        }
        if (!Double.isFinite(value)) {
            return reject(RejectReason.BAD_VALUE, "value 必须有限: " + value);
        }

        Reading reading = new Reading(sourceId, pointId, seq, ts, value);
        log.info("[normalizer:csv] 归一成功 -> {}", reading);
        return ParseOutcome.ok(reading);
    }

    private static Long parseLong(String raw, String field) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double parseDouble(String raw) {
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ParseOutcome reject(RejectReason reason, String detail) {
        log.info("[normalizer:csv] 判定拒绝 reason={}, detail={}", reason, detail);
        return ParseOutcome.reject(reason, detail);
    }
}
