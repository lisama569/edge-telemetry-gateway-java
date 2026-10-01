package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Downstream;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.DownstreamTopology;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.Reconciliation;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream.SimulatedDownstreamChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResponse;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地验证用 REST 控制面：
 * - 注入报文（返回与输入一一对应的可读判定 ACCEPTED/DUPLICATE/LATE/REJECTED+原因）；
 * - 按下游名（cloud/archive）模拟通道抖动、启停转发循环；
 * - 分下游查看可观测计数与对账结果，双下游一致性可查询。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final IngestService ingestService;
    private final DownstreamTopology topology;

    public GatewayController(IngestService ingestService, DownstreamTopology topology) {
        this.ingestService = ingestService;
        this.topology = topology;
    }

    @PostMapping("/ingest")
    public IngestResponse ingest(@RequestBody RawFrame frame) {
        return IngestResponse.from(ingestService.ingest(frame));
    }

    @PostMapping("/downstreams/{name}/channel/up")
    public Map<String, Object> channelUp(@PathVariable String name) {
        channel(name).bringUp();
        return Map.of("downstream", name, "channelUp", true);
    }

    @PostMapping("/downstreams/{name}/channel/down")
    public Map<String, Object> channelDown(@PathVariable String name) {
        channel(name).bringDown();
        return Map.of("downstream", name, "channelUp", false);
    }

    @PostMapping("/downstreams/{name}/forwarder/start")
    public Map<String, Object> start(@PathVariable String name) {
        topology.get(name).forwarder().start();
        return Map.of("downstream", name, "forwarder", "started");
    }

    @PostMapping("/downstreams/{name}/forwarder/stop")
    public Map<String, Object> stop(@PathVariable String name) {
        topology.get(name).forwarder().stop();
        return Map.of("downstream", name, "forwarder", "stopped");
    }

    /** 手动触发某下游转发一轮（重复触发不会重发已确认数据）。 */
    @PostMapping("/downstreams/{name}/flush")
    public Map<String, Object> flush(@PathVariable String name) {
        int n = topology.get(name).forwarder().flushOnce();
        return Map.of("downstream", name, "confirmed", n);
    }

    // ---- 兼容原单出口控制面（作用于 cloud） ----

    @PostMapping("/channel/up")
    public Map<String, Object> channelUpLegacy() {
        channel("cloud").bringUp();
        return Map.of("channelUp", true);
    }

    @PostMapping("/channel/down")
    public Map<String, Object> channelDownLegacy() {
        channel("cloud").bringDown();
        return Map.of("channelUp", false);
    }

    @PostMapping("/forwarder/start")
    public Map<String, Object> startLegacy() {
        topology.get("cloud").forwarder().start();
        return Map.of("forwarder", "started");
    }

    @PostMapping("/forwarder/stop")
    public Map<String, Object> stopLegacy() {
        topology.get("cloud").forwarder().stop();
        return Map.of("forwarder", "stopped");
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ingest.accepted", ingestService.acceptedCount());
        m.put("ingest.duplicates", ingestService.duplicateCount());
        m.put("ingest.late", ingestService.lateCount());
        Map<String, Long> rejected = new LinkedHashMap<>();
        for (RejectReason reason : RejectReason.values()) {
            rejected.put(reason.name(), ingestService.rejectedCount(reason));
        }
        m.put("ingest.rejected", rejected);

        Map<String, Object> downstreams = new LinkedHashMap<>();
        for (Downstream d : topology.all()) {
            downstreams.put(d.name(), downstreamMetrics(d));
        }
        m.put("downstreams", downstreams);
        return m;
    }

    /** 分下游对账：追加/接收/缓存/淘汰/重复/迟到计数，以及平衡判定。 */
    @GetMapping("/reconciliation")
    public Map<String, Object> reconciliation() {
        Map<String, Object> m = new LinkedHashMap<>();
        for (Downstream d : topology.all()) {
            SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
            long received = ch.received().size();
            long buffered = d.buffer().size();
            long dropped = d.buffer().droppedCount();
            long appended = received + buffered + dropped;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("appended", appended);
            row.put("receivedUnique", received);
            row.put("buffered", buffered);
            row.put("dropped", dropped);
            row.put("duplicateDeliveries", ch.receiver().duplicateCount());
            row.put("lateAccepted", ch.receiver().lateAcceptedCount());
            row.put("balanced", Reconciliation.balanced(appended, received, buffered, dropped));
            m.put(d.name(), row);
        }
        if (topology.all().size() == 2) {
            List<TelemetryPoint> cloud = ((SimulatedDownstreamChannel) topology.get("cloud").channel()).received();
            List<TelemetryPoint> archive = ((SimulatedDownstreamChannel) topology.get("archive").channel()).received();
            m.put("cloudArchiveDiffs", Reconciliation.diffByPoint(cloud, archive));
        }
        return m;
    }

    private Map<String, Object> downstreamMetrics(Downstream d) {
        SimulatedDownstreamChannel ch = (SimulatedDownstreamChannel) d.channel();
        Map<String, Object> row = new LinkedHashMap<>(Reconciliation.snapshot(d));
        row.put("channelUp", ch.isUp());
        row.put("sendAttempts", ch.sendAttempts());
        row.put("sendFailures", ch.sendFailures());
        return row;
    }

    private SimulatedDownstreamChannel channel(String name) {
        return (SimulatedDownstreamChannel) topology.get(name).channel();
    }
}
