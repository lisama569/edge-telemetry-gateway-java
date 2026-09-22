package com.github.lisama569.edgetelemetrygatewayjava.normalizer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * JSON 报文归一化器。
 *
 * 期望格式（字段名固定，多余字段忽略）：
 * <pre>
 * {"sourceId":"sensor-A","pointId":"temperature","seq":1,"timestampMs":1727000000000,"value":23.5}
 * </pre>
 * 拒绝原因区分：
 * - MALFORMED：不是合法 JSON 或根节点不是对象；
 * - MISSING_FIELD：缺少必填字段或字段为空；
 * - BAD_VALUE：字段类型/取值非法（含 NaN、无穷、负序号/负时间戳）。
 */
@Component
public class JsonReadingNormalizer implements ProtocolNormalizer {

    private static final Logger log = LoggerFactory.getLogger(JsonReadingNormalizer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String protocol() {
        return "json";
    }

    @Override
    public ParseOutcome parse(String raw) {
        log.info("[normalizer:json] 输入报文原文: {}", raw);
        if (raw == null || raw.isBlank()) {
            return reject(RejectReason.MALFORMED, "报文为空");
        }
        final JsonNode root;
        try {
            root = MAPPER.readTree(raw);
        } catch (JacksonException e) {
            return reject(RejectReason.MALFORMED, "JSON 语法错误: " + e.getOriginalMessage());
        }
        if (root == null || !root.isObject()) {
            return reject(RejectReason.MALFORMED, "根节点必须是 JSON 对象");
        }

        JsonNode sourceNode = root.get("sourceId");
        JsonNode pointNode = root.get("pointId");
        if (sourceNode == null || sourceNode.isNull()) {
            return reject(RejectReason.MISSING_FIELD, "缺少字段 sourceId");
        }
        if (pointNode == null || pointNode.isNull()) {
            return reject(RejectReason.MISSING_FIELD, "缺少字段 pointId");
        }
        if (!sourceNode.isTextual()) {
            return reject(RejectReason.BAD_VALUE, "sourceId 必须是字符串, 实际=" + sourceNode);
        }
        if (!pointNode.isTextual()) {
            return reject(RejectReason.BAD_VALUE, "pointId 必须是字符串, 实际=" + pointNode);
        }
        String sourceId = sourceNode.asText();
        String pointId = pointNode.asText();
        if (sourceId.isBlank()) {
            return reject(RejectReason.MISSING_FIELD, "sourceId 为空白");
        }
        if (pointId.isBlank()) {
            return reject(RejectReason.MISSING_FIELD, "pointId 为空白");
        }

        JsonNode seqNode = root.get("seq");
        if (seqNode == null || seqNode.isNull()) {
            return reject(RejectReason.MISSING_FIELD, "缺少字段 seq");
        }
        JsonNode tsNode = root.get("timestampMs");
        if (tsNode == null || tsNode.isNull()) {
            return reject(RejectReason.MISSING_FIELD, "缺少字段 timestampMs");
        }
        JsonNode valueNode = root.get("value");
        if (valueNode == null || valueNode.isNull()) {
            return reject(RejectReason.MISSING_FIELD, "缺少字段 value");
        }
        if (!seqNode.isNumber() || !seqNode.canConvertToLong()) {
            return reject(RejectReason.BAD_VALUE, "seq 必须是整数, 实际=" + seqNode);
        }
        if (!tsNode.isNumber() || !tsNode.canConvertToLong()) {
            return reject(RejectReason.BAD_VALUE, "timestampMs 必须是整数, 实际=" + tsNode);
        }
        if (!valueNode.isNumber()) {
            return reject(RejectReason.BAD_VALUE, "value 必须是数值, 实际=" + valueNode);
        }
        long seq = seqNode.asLong();
        long ts = tsNode.asLong();
        double value = valueNode.asDouble();
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
        log.info("[normalizer:json] 归一成功 -> {}", reading);
        return ParseOutcome.ok(reading);
    }

    private static ParseOutcome reject(RejectReason reason, String detail) {
        log.info("[normalizer:json] 判定拒绝 reason={}, detail={}", reason, detail);
        return ParseOutcome.reject(reason, detail);
    }
}
