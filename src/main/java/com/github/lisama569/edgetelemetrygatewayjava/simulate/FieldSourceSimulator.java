package com.github.lisama569.edgetelemetrygatewayjava.simulate;

import com.github.lisama569.edgetelemetrygatewayjava.ingest.IngestService;
import com.github.lisama569.edgetelemetrygatewayjava.model.IngestResult;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 本地现场数据源模拟器。burst() 确定性地产生一轮包含正常、重复、
 * 乱序/迟到、畸形、缺字段、坏值、未知协议的报文，便于本地与自动化验证。
 */
@Component
public class FieldSourceSimulator {

    private static final Logger log = LoggerFactory.getLogger(FieldSourceSimulator.class);

    private final IngestService ingestService;

    public FieldSourceSimulator(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /** 一条待发送的模拟报文。 */
    public record RawMessage(String protocol, String payload, String intent) {
    }

    /**
     * 构造一轮固定报文（内容与顺序确定，结论可复现）。
     */
    public static List<RawMessage> scriptedBurst() {
        List<RawMessage> messages = new ArrayList<>();
        long t = 1_727_000_000_000L;

        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":1,"
                        + "\"timestampMs\":" + t + ",\"value\":23.1}", "json 正常 seq=1"));
        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":2,"
                        + "\"timestampMs\":" + (t + 1) + ",\"value\":23.4}", "json 正常 seq=2"));
        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":1,"
                        + "\"timestampMs\":" + t + ",\"value\":23.1}", "重复 seq=1"));
        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":2,"
                        + "\"timestampMs\":" + (t + 1) + ",\"value\":99.9}", "迟到旧报文 seq=2（值不同也不得覆盖）"));
        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":3,"
                        + "\"timestampMs\":" + (t + 2) + ",\"value\":23.9}", "乱序后到达的新值 seq=3"));
        messages.add(new RawMessage("csv",
                "sensor-B,pressure,10," + (t + 10) + ",1.05", "csv 正常 seq=10"));
        messages.add(new RawMessage("csv",
                "sensor-B,pressure,11," + (t + 11) + ",1.07", "csv 正常 seq=11"));
        messages.add(new RawMessage("json",
                "this-is-not-json", "畸形 JSON"));
        messages.add(new RawMessage("json",
                "{\"sourceId\":\"sensor-A\",\"pointId\":\"temperature\",\"seq\":4,"
                        + "\"timestampMs\":" + (t + 3) + "}", "缺少 value 字段"));
        messages.add(new RawMessage("csv",
                "sensor-B,pressure,abc," + (t + 12) + ",1.08", "seq 非数字（坏值）"));
        messages.add(new RawMessage("modbus",
                "anything", "未知协议"));
        return messages;
    }

    /**
     * 发送一轮脚本报文并逐条收集结论。
     */
    public List<IngestResult> burst() {
        List<IngestResult> results = new ArrayList<>();
        for (RawMessage msg : scriptedBurst()) {
            log.info("[simulate] 发送报文 intent='{}' protocol={} payload={}",
                    msg.intent(), msg.protocol(), msg.payload());
            IngestResult result = ingestService.ingest(msg.protocol(), msg.payload());
            log.info("[simulate] 处置结论 intent='{}' -> outcome={} reason={} entryId={} detail={}",
                    msg.intent(), result.outcome(), result.rejectReason(),
                    result.entryId(), result.detail());
            results.add(result);
        }
        return results;
    }
}
