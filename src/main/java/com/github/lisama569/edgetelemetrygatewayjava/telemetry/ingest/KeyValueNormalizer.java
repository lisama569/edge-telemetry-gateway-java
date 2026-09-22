package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * "kv" 协议归一器。
 * 报文格式: source=<id>;point=<id>;seq=<long>;ts=<epochMillis|ISO-8601>;value=<double>[;quality=GOOD|BAD|UNCERTAIN]
 *
 * 拒绝原因可区分：
 * - MALFORMED_PAYLOAD: 结构非法（缺字段、字段无法解析、空报文）
 * - INVALID_FIELD:     结构合法但取值非法（seq<0、value 非有限数、quality 未知）
 */
@Component
public class KeyValueNormalizer implements ProtocolNormalizer {

    private static final Logger log = LoggerFactory.getLogger(KeyValueNormalizer.class);

    @Override
    public String protocol() {
        return "kv";
    }

    @Override
    public TelemetryPoint normalize(RawFrame frame) throws NormalizationException {
        String payload = frame.payload();
        if (payload == null || payload.isBlank()) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "empty payload");
        }
        Map<String, String> fields = new HashMap<>();
        for (String pair : payload.split(";")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD,
                        "bad field segment: '" + pair + "'");
            }
            fields.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        String source = required(fields, "source");
        String point = required(fields, "point");
        long seq = parseSeq(fields.get("seq"));
        Instant ts = parseTs(fields.get("ts"));
        double value = parseValue(fields.get("value"));
        TelemetryPoint.Quality quality = parseQuality(fields.get("quality"));

        TelemetryPoint tp = new TelemetryPoint(source, point, seq, ts, value, quality);
        log.debug("normalized frame -> {}", tp);
        return tp;
    }

    private String required(Map<String, String> fields, String name) throws NormalizationException {
        String v = fields.get(name);
        if (v == null || v.isEmpty()) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "missing field: " + name);
        }
        return v;
    }

    private long parseSeq(String raw) throws NormalizationException {
        if (raw == null) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "missing field: seq");
        }
        try {
            long seq = Long.parseLong(raw);
            if (seq < 0) {
                throw new NormalizationException(RejectReason.INVALID_FIELD, "seq must be >= 0, got " + seq);
            }
            return seq;
        } catch (NumberFormatException e) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "seq not a long: '" + raw + "'");
        }
    }

    private Instant parseTs(String raw) throws NormalizationException {
        if (raw == null) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "missing field: ts");
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(raw));
        } catch (NumberFormatException ignored) {
            // fall through to ISO-8601
        }
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException e) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "ts not epochMillis/ISO-8601: '" + raw + "'");
        }
    }

    private double parseValue(String raw) throws NormalizationException {
        if (raw == null) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "missing field: value");
        }
        try {
            double v = Double.parseDouble(raw);
            if (!Double.isFinite(v)) {
                throw new NormalizationException(RejectReason.INVALID_FIELD, "value not finite: '" + raw + "'");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new NormalizationException(RejectReason.MALFORMED_PAYLOAD, "value not a double: '" + raw + "'");
        }
    }

    private TelemetryPoint.Quality parseQuality(String raw) throws NormalizationException {
        if (raw == null || raw.isEmpty()) {
            return TelemetryPoint.Quality.GOOD;
        }
        try {
            return TelemetryPoint.Quality.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new NormalizationException(RejectReason.INVALID_FIELD, "unknown quality: '" + raw + "'");
        }
    }
}
