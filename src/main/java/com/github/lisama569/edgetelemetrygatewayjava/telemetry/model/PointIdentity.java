package com.github.lisama569.edgetelemetrygatewayjava.telemetry.model;

/**
 * 测点数据的业务身份：同一来源、同一测点、同一序号唯一标识一条现场数据。
 * 下游据此做幂等判定，与值内容无关，重启后仍可识别重复投递。
 */
public record PointIdentity(String sourceId, String pointId, long seq) {

    public static PointIdentity of(TelemetryPoint point) {
        return new PointIdentity(point.sourceId(), point.pointId(), point.seq());
    }

    /** (sourceId, pointId) 测点键。 */
    public String pointKey() {
        return sourceId + "/" + pointId;
    }
}
