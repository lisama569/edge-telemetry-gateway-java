package com.github.lisama569.edgetelemetrygatewayjava.normalizer;

import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import com.github.lisama569.edgetelemetrygatewayjava.model.RejectReason;

/**
 * 单种报文协议的归一化器。
 */
public interface ProtocolNormalizer {

    String protocol();

    /**
     * 解析原始报文。失败时在 rejectReason 中给出明确原因。
     */
    ParseOutcome parse(String raw);

    record ParseOutcome(Reading reading, RejectReason rejectReason, String detail) {
        public static ParseOutcome ok(Reading r) { return new ParseOutcome(r, null, null); }
        public static ParseOutcome reject(RejectReason reason, String detail) {
            return new ParseOutcome(null, reason, detail);
        }
        public boolean accepted() { return reading != null; }
    }
}
