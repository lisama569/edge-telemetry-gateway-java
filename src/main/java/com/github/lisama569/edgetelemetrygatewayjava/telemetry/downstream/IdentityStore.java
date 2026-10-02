package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;

import java.util.List;

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

    /** 已真正接收的不同身份总数（持久账本跨重启保留；进程内账本随进程清零）。 */
    long size();

    /**
     * 某测点已真正接收的全部身份，按 seq 升序（用于跨重启的逐测点序列对账）。
     */
    List<PointIdentity> identitiesForPoint(String pointKey);

    /** 账本中出现过的全部测点键（来自水位表，含已全部确认、重启后无在途数据的测点）。 */
    java.util.Set<String> pointKeys();
}
