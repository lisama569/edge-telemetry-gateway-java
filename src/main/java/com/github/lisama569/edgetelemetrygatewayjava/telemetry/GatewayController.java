package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout.FanOutConfig;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout.FanOutForwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout.IdempotentSimulatedChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DecisionLog;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DecisionRecord;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.RejectReason;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地验证用 REST 控制面：注入报文、模拟双下游抖动、逐条查询判定结论、查看可观测计数。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final IngestService ingestService;
    private final DecisionLog decisionLog;
    private final FanOutForwarder forwarder;
    private final TelemetryBuffer buffer;

    public GatewayController(IngestService ingestService,
                             DecisionLog decisionLog,
                             FanOutForwarder forwarder,
                             TelemetryBuffer buffer) {
        this.ingestService = ingestService;
        this.decisionLog = decisionLog;
        this.forwarder = forwarder;
        this.buffer = buffer;
    }

    /** 注入报文；返回 decisionId 与判定结论，输入与判定一一对应。 */
    @PostMapping("/ingest")
    public Map<String, Object> ingest(@RequestBody RawFrame frame) {
        IngestResult result = ingestService.ingest(frame);
        DecisionRecord rec = decisionLog.recent(1).getLast();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("decisionId", rec.decisionId());
        body.put("verdict", rec.verdict());
        body.put("reason", rec.reason());
        body.put("detail", rec.detail());
        body.put("result", result.toString());
        return body;
    }

    /** 按 decisionId 单独查询这条注入的判定结论。 */
    @GetMapping("/decisions/{decisionId}")
    public ResponseEntity<DecisionRecord> decision(@PathVariable String decisionId) {
        return decisionLog.find(decisionId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** 最近判定列表（默认 20 条），用于一眼区分接收/重复/迟到/各类拒绝。 */
    @GetMapping("/decisions")
    public List<DecisionRecord> decisions(@RequestParam(defaultValue = "20") int limit) {
        return decisionLog.recent(limit);
    }

    /** 立即触发某下游（cloud/archive）转发一轮，便于手工复现，不依赖后台循环。 */
    @PostMapping("/forward/flush")
    public Map<String, Object> flush(@RequestParam(defaultValue = "cloud") String downstream) {
        int advanced = forwarder.flushOnce(downstream);
        return Map.of("downstream", downstream, "advanced", advanced,
                "ackedOffset", forwarder.ackedOffset(downstream));
    }

    @PostMapping("/channel/up")
    public Map<String, Object> channelUp(@RequestParam(defaultValue = "cloud") String downstream) {
        channel(downstream).bringUp();
        return Map.of("downstream", downstream, "up", true);
    }

    @PostMapping("/channel/down")
    public Map<String, Object> channelDown(@RequestParam(defaultValue = "cloud") String downstream) {
        channel(downstream).bringDown();
        return Map.of("downstream", downstream, "up", false);
    }

    @PostMapping("/forwarder/start")
    public Map<String, Object> startForwarder() {
        forwarder.start();
        return Map.of("forwarder", "started", "downstreams", forwarder.downstreamNames());
    }

    @PostMapping("/forwarder/stop")
    public Map<String, Object> stopForwarder() {
        forwarder.stop();
        return Map.of("forwarder", "stopped");
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ingest.accepted", ingestService.acceptedCount());
        m.put("ingest.duplicates", ingestService.duplicateCount());
        m.put("ingest.late", ingestService.lateCount());
        for (RejectReason reason : RejectReason.values()) {
            m.put("ingest.rejected." + reason.name(), ingestService.rejectedCount(reason));
        }
        m.put("buffer.size", buffer.size());
        m.put("buffer.dropped", buffer.droppedCount());
        m.put("buffer.rejectedFull", buffer.rejectedFullCount());
        for (String name : List.of(FanOutConfig.CLOUD, FanOutConfig.ARCHIVE)) {
            IdempotentSimulatedChannel ch = forwarder.channel(name);
            Map<String, Object> dm = new LinkedHashMap<>();
            dm.put("up", ch.isUp());
            dm.put("ackedOffset", forwarder.ackedOffset(name));
            dm.put("attemptedBatched", forwarder.flushedTotal(name));
            dm.put("duplicateDeliveries", forwarder.duplicateDeliveries(name));
            dm.put("failedRounds", forwarder.failedRounds(name));
            dm.put("skippedDown", forwarder.skippedDown(name));
            dm.put("acceptedTotal", ch.acceptedTotal());
            dm.put("received", ch.received().size());
            dm.put("sendFailures", ch.sendFailures());
            m.put("downstream." + name, dm);
        }
        return m;
    }

    private IdempotentSimulatedChannel channel(String downstream) {
        IdempotentSimulatedChannel ch = forwarder.channel(downstream);
        if (ch == null) {
            throw new IllegalArgumentException("unknown downstream: " + downstream);
        }
        return ch;
    }
}
