# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路的本地模拟实现：现场数据源与云端通道均由本地组件模拟，
采集、缓存、转发各环节在异常与恢复下的行为确定、可观测、结论可复现。

## 链路结构

```
SimulatedSource ──RawFrame──> IngestService ──TelemetryPoint──> FileTelemetryBuffer ──> Forwarder ──> SimulatedCloudChannel
                              (协议归一 + 去重乱序判定)            (本地持久化缓存)          (单消费循环,批量 ack)
```

## 协议归一规则

- 统一模型 `TelemetryPoint(sourceId, pointId, seq, sampledAt, value, quality)`，
  测点由 `(sourceId, pointId)` 唯一标识，`seq` 为来源侧单调递增序号。
- 归一器按协议注册（`ProtocolNormalizer`），当前内置 `kv` 文本协议：
  `source=<id>;point=<id>;seq=<long>;ts=<epochMillis|ISO-8601>;value=<double>[;quality=GOOD|BAD|UNCERTAIN]`
- 拒绝原因可区分（`RejectReason`）：
  - `UNKNOWN_PROTOCOL`：协议类型未注册；
  - `MALFORMED_PAYLOAD`：结构非法（空报文、缺字段、字段无法解析）；
  - `INVALID_FIELD`：结构合法但取值非法（`seq<0`、`value` 非有限数、`quality` 未知）。
- 去重与乱序判定（`DedupGate`，按测点维护水位，线程安全）：
  - `seq > lastSeq` → **Accepted**，推进水位；
  - `seq == lastSeq` 且内容一致 → **Duplicate**，丢弃不重复计数；
  - `seq < lastSeq`，或 `seq == lastSeq` 但内容冲突 → **Late**，绝不覆盖已确认数据。

## 缓存与续传语义

- `FileTelemetryBuffer` 以 JSON Lines 落盘（默认 `./data/buffer/buffer.jsonl`，
  容量 `telemetry.buffer.capacity`，默认 10000），每次变更先写临时文件再原子替换。
- **容量溢出**：行为确定 —— 淘汰最旧一条（`APPENDED_EVICTED_OLDEST`），
  `droppedCount` 递增并打 WARN 日志，绝不静默丢弃。
- **确认语义**：`peekBatch` 只读、`ack(count)` 在转发成功后从队首移除并立即落盘。
  已确认数据重启后不会重发；未确认数据重启后自动恢复续传（at-least-once，
  云端侧可凭 `(sourceId, pointId, seq)` 去重）。

## 背压与并发处置

- 采集侧写入只受缓存容量约束；容量满时走上述确定性溢出路径，并给出 WARN 日志与计数。
- 转发侧为**单线程消费循环**（`Forwarder`，批次 `telemetry.forward.batch-size`，
  间隔 `telemetry.forward.interval-millis`）：队首取批 → 发送 → 成功后 `ack`。
  单消费 + 队首确认保证转发顺序与缓存顺序一致，多轮重试/重启后结论可复现。
- 通道 down 或发送失败时不 `ack`，数据保留待重试；判定可观测：
  `skippedChannelDown`、`failedRounds`、`flushedTotal` 等计数器及日志。
- 对账不变式（测试断言）：`accepted == received + buffered + dropped`。

## 本地验证

### 单元测试（日志打印输入报文与关键判定）

```bash
mvn test
# 只看某个场景：
mvn test -Dtest=IngestServiceTest          # 乱序/重复/迟到/拒绝原因
mvn test -Dtest=BufferOverflowTest         # 缓存溢出与重启恢复
mvn test -Dtest=ChannelRecoveryTest        # 通道抖动、恢复续传、重启不丢不重
mvn test -Dtest=BackpressureConcurrencyTest # 并发采集 + 慢通道背压对账
```

### 运行期 REST 控制面

```bash
mvn spring-boot:run

# 注入报文
curl -X POST localhost:8080/api/ingest -H 'Content-Type: application/json' \
  -d '{"protocol":"kv","payload":"source=plc-1;point=temperature;seq=1;ts=1758000000000;value=23.5;quality=GOOD"}'

# 模拟通道抖动与恢复
curl -X POST localhost:8080/api/channel/down
curl -X POST localhost:8080/api/channel/up

# 启动/停止后台转发循环，查看可观测计数
curl -X POST localhost:8080/api/forwarder/start
curl localhost:8080/api/metrics
```
