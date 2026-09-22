package com.github.lisama569.edgetelemetrygatewayjava.web;

import com.github.lisama569.edgetelemetrygatewayjava.cache.PendingStore;
import com.github.lisama569.edgetelemetrygatewayjava.forward.AbstractSimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.CloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.forward.ForwardingService;
import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.metrics.GatewayMetrics;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.simulate.FieldSourceSimulator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地验证用 HTTP 接口：
 * <ul>
 *   <li>POST /ingest/{protocol}：投递一条原始报文；</li>
 *   <li>GET  /metrics：全链路计数与缓存占用；</li>
 *   <li>GET  /forward/status：各通道可用性与水位；</li>
 *   <li>POST /simulate/burst：发送一轮含重复/乱序/非法报文的模拟数据；</li>
 *   <li>POST /channels/{name}/availability?available=true|false：模拟通道断连/恢复；</li>
 *   <li>POST /channels/{name}/fail-next?n=2：让接下来 n 次投递确定性失败；</li>
 *   <li>POST /channels/{name}/delay?ms=500：设置投递延迟模拟慢通道。</li>
 * </ul>
 */
@RestController
public class GatewayController {

    private final IngestService ingestService;
    private final FieldSourceSimulator simulator;
    private final GatewayMetrics metrics;
    private final PendingStore store;
    private final List<CloudChannel> channels;
    private final ForwardingService forwardingService;

    public GatewayController(IngestService ingestService, FieldSourceSimulator simulator,
                             GatewayMetrics metrics, PendingStore store,
                             List<CloudChannel> channels, ForwardingService forwardingService) {
        this.ingestService = ingestService;
        this.simulator = simulator;
        this.metrics = metrics;
        this.store = store;
        this.channels = channels;
        this.forwardingService = forwardingService;
    }

    @PostMapping("/ingest/{protocol}")
    public IngestResult ingest(@PathVariable String protocol, @RequestBody String raw) {
        return ingestService.ingest(protocol, raw);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> snapshot = new LinkedHashMap<>(metrics.snapshot());
        snapshot.put("cachePending", store.pendingCount());
        snapshot.put("cacheCapacity", store.capacity());
        snapshot.put("lastEntryId", store.lastEntryId());
        return snapshot;
    }

    @GetMapping("/forward/status")
    public Map<String, Object> forwardStatus() {
        return forwardingService.status();
    }

    @PostMapping("/simulate/burst")
    public List<IngestResult> burst() {
        return simulator.burst();
    }

    @PostMapping("/channels/{name}/availability")
    public Map<String, Object> channelAvailability(@PathVariable String name,
                                                   @RequestParam boolean available) {
        AbstractSimulatedCloudChannel channel = requireChannel(name);
        channel.setAvailable(available);
        forwardingService.kick();
        return Map.of("channel", name, "available", channel.available());
    }

    @PostMapping("/channels/{name}/fail-next")
    public Map<String, Object> failNext(@PathVariable String name, @RequestParam int n) {
        AbstractSimulatedCloudChannel channel = requireChannel(name);
        channel.failNextDeliveries(n);
        forwardingService.kick();
        return Map.of("channel", name, "scheduledFailures", n);
    }

    @PostMapping("/channels/{name}/delay")
    public Map<String, Object> delay(@PathVariable String name, @RequestParam long ms) {
        AbstractSimulatedCloudChannel channel = requireChannel(name);
        channel.setDelayMs(ms);
        return Map.of("channel", name, "delayMs", ms);
    }

    private AbstractSimulatedCloudChannel requireChannel(String name) {
        Optional<CloudChannel> found = channels.stream().filter(c -> c.name().equals(name)).findFirst();
        if (found.isEmpty() || !(found.get() instanceof AbstractSimulatedCloudChannel simulated)) {
            throw new IllegalArgumentException("未知通道: " + name);
        }
        return simulated;
    }
}
