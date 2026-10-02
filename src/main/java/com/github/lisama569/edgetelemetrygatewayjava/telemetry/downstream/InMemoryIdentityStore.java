package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    @Override
    public long size() {
        return received.size();
    }

    @Override
    public List<PointIdentity> identitiesForPoint(String pointKey) {
        List<PointIdentity> ids = new ArrayList<>();
        for (PointIdentity id : received) {
            if (id.pointKey().equals(pointKey)) {
                ids.add(id);
            }
        }
        ids.sort(Comparator.comparingLong(PointIdentity::seq));
        return ids;
    }

    @Override
    public Set<String> pointKeys() {
        return java.util.Set.copyOf(watermarks.keySet());
    }
}
