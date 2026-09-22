package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.Forwarder;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward.SimulatedCloudChannel;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestResult;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.RawFrame;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地验证用 REST 控制面：注入报文、模拟通道抖动、查看可观测计数。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final IngestService ingestService;
    private final Forwarder forwarder;
    private final TelemetryBuffer buffer;
    private final SimulatedCloudChannel channel;

    public GatewayController(IngestService ingestService,
                             Forwarder forwarder,
                             TelemetryBuffer buffer,
                             SimulatedCloudChannel channel) {
        this.ingestService = ingestService;
        this.forwarder = forwarder;
        this.buffer = buffer;
        this.channel = channel;
    }

    @PostMapping("/ingest")
    public IngestResult ingest(@RequestBody RawFrame frame) {
        return ingestService.ingest(frame);
    }

    @PostMapping("/channel/up")
    public Map<String, Object> channelUp() {
        channel.bringUp();
        return Map.of("channelUp", true);
    }

    @PostMapping("/channel/down")
    public Map<String, Object> channelDown() {
        channel.bringDown();
        return Map.of("channelUp", false);
    }

    @PostMapping("/forwarder/start")
    public Map<String, Object> startForwarder() {
        forwarder.start();
        return Map.of("forwarder", "started");
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
        m.put("buffer.size", buffer.size());
        m.put("buffer.dropped", buffer.droppedCount());
        m.put("forward.flushedTotal", forwarder.flushedTotal());
        m.put("forward.failedRounds", forwarder.failedRounds());
        m.put("forward.skippedChannelDown", forwarder.skippedChannelDown());
        m.put("channel.up", channel.isUp());
        m.put("channel.received", channel.received().size());
        m.put("channel.sendFailures", channel.sendFailures());
        return m;
    }
}
