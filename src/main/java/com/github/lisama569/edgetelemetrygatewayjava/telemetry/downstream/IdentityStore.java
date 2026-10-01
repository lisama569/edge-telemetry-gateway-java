package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;

/**
 * 下游已接收身份的持久化存储（模拟下游侧的幂等账本）。
 * 网关重启后账本仍在，因此崩溃续传的重复投递可被识别。
 */
public interface IdentityStore {

    /** 该身份是否已被真正接收。 */
    boolean contains(PointIdentity identity);

    /** 记录某测点最新已接收水位（用于区分真实迟到数据）。 */
    long watermark(String pointKey);

    /** 真正接收：登记身份并推进水位。 */
    void accept(PointIdentity identity);
}
