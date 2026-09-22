package com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest;

public class NormalizationException extends Exception {

    private final RejectReason reason;

    public NormalizationException(RejectReason reason, String detail) {
        super(detail);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
