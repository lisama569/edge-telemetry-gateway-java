package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内身份账本：用于单进程测试与纯内存场景；重启后账本清空（不模拟崩溃续传）。
 */
public class InMemoryIdentityStore implements IdentityStore {

    private final Set<PointIdentity> received = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();

    @Override
    public boolean contains(PointIdentity identity) {
        return received.contains(identity);
    }

    @Override
    public long watermark(String pointKey) {
        return watermarks.getOrDefault(pointKey, -1L);
    }

    @Override
    public void accept(PointIdentity identity) {
        received.add(identity);
        watermarks.merge(identity.pointKey(), identity.seq(), Math::max);
    }
}
