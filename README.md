# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路的本地模拟实现：现场数据源与下游通道均由本地组件模拟，
采集、缓存、转发各环节在异常、双下游、崩溃恢复下的行为确定、可观测、结论可复现。

## 链路结构

```
SimulatedSource ──RawFrame──> IngestService ──TelemetryPoint──> FileTelemetryBuffer
                              (协议归一+去重乱序判定+DecisionLog)        (有界持久化,多消费者游标)
                                                                          │
                                   ┌──────────────────────────────────────┴──────────────────────────────┐
                                   ▼                                                                        ▼
                        FanOutForwarder 线程: cloud                                          FanOutForwarder 线程: archive
                        IdempotentSimulatedChannel(云端出口,台账)                             IdempotentSimulatedChannel(本地归档旁路,台账)
```

同一份现场数据并行发往两个独立下游（云端出口 `cloud` + 本地归档 `archive`），
各自独立消费、各自按数据身份幂等接收。

## 协议归一规则

- 统一模型 `TelemetryPoint(sourceId, pointId, seq, sampledAt, value, quality)`，
  测点由 `(sourceId, pointId)` 唯一标识，`seq` 为来源侧单调递增序号。
- 归一器按协议注册（`ProtocolNormalizer`），当前内置 `kv` 文本协议：
  `source=<id>;point=<id>;seq=<long>;ts=<epochMillis|ISO-8601>;value=<double>[;quality=GOOD|BAD|UNCERTAIN]`
- 采集侧拒绝原因（`RejectReason`）：
  - `UNKNOWN_PROTOCOL`：协议类型未注册；
  - `MALFORMED_PAYLOAD`：结构非法（空报文、缺字段、字段无法解析）；
  - `INVALID_FIELD`：结构合法但取值非法（`seq<0`、`value` 非有限数、`quality` 未知）；
  - `BUFFER_FULL`：多下游模式下缓存到上限且队首仍被慢下游占用，本次写入被确定性拒绝。
- 去重与乱序判定（`DedupGate`，按测点维护水位，线程安全）：
  - `seq > lastSeq` → **ACCEPTED**，推进水位；
  - `seq == lastSeq` 且内容一致 → **DUPLICATE**，丢弃不计数；
  - `seq < lastSeq`，或 `seq == lastSeq` 但内容冲突 → **LATE**，绝不覆盖已确认数据。

## 判定结果可区分、可统计、可查询（新增）

每次注入产生一条 `DecisionRecord`，输入报文与判定结论一一对应：

| verdict | 含义 | reason 字段 |
|---|---|---|
| `ACCEPTED` | 正常接收并入缓存 | — |
| `DUPLICATE` | 同测点同 seq 且内容一致的重发 | — |
| `LATE` | seq 回退或同 seq 内容冲突 | 落后水位说明 |
| `REJECTED` | 未通过归一/容量校验 | `UNKNOWN_PROTOCOL` / `MALFORMED_PAYLOAD` / `INVALID_FIELD` / `BUFFER_FULL` |

- 每条记录有唯一 `decisionId`（`d-000001`…），响应体直接返回，可 `GET /api/decisions/{id}` 单独查询；
- `GET /api/decisions?limit=N` 查看最近判定序列；
- 计数器按结论与各拒绝原因分别统计（`/api/metrics` 中 `ingest.*` / `ingest.rejected.*`）。

## 缓存与双下游续传语义（增强）

`FileTelemetryBuffer` 以 JSON Lines 落盘（`buffer.jsonl`，每行一个 `{offset, point}` 信封，
`offset` 为全局单调身份序号），另有 `progress.json` 记录各下游已确认游标；
每次变更先写临时文件再原子替换。容量由 `telemetry.buffer.capacity` 配置（默认 10000）。

### 传统单出口模式（向后兼容）

未注册任何消费者时，行为与此前完全一致：

- **容量溢出**：确定性淘汰最旧一条（`APPENDED_EVICTED_OLDEST`），`droppedCount` 递增并 WARN；
- `peekBatch`/`ack(count)` 语义不变；原有单测（`BufferOverflowTest` 等）不改一行继续通过；
- 旧的 `Forwarder` + `SimulatedCloudChannel` 类保留（不再作为 Spring Bean），
  可用 `new` 直接构造复现单出口 at-least-once 行为。

### 多下游模式（新增，Spring 运行期默认）

- `registerConsumer(name)` 注册下游，各自维护 `ackedOffset` 游标，游标随 `progress.json` 持久化；
- 每个下游独立单线程消费循环（`FanOutForwarder`），只推进自己的游标：
  - 某下游变慢 / down / 失败时不 ack，数据保留，按自身进度续传，**不阻塞另一个下游**；
  - 只有**所有**下游都确认过的队首数据才物理移除，慢下游不会漏收，快下游不会多收；
- **对账不变式**：两下游收到的测点身份集合、顺序、条数完全一致（测试逐点断言）；
- **容量上限处置确定**：容量满且队首仍被任一下游占用时，新写入确定性 `REJECTED_BUFFER_FULL`
  （不淘汰、不入任何下游、计数 + WARN），保证双下游集合一致且行为可预期；
  恢复（慢下游追平、队列腾出空间）后新数据可正常写入；
- 重复触发 flush / 重启：游标只在整批成功后推进，已确认数据绝不重发。

## 崩溃续传下的幂等去重（新增）

`IdempotentSimulatedChannel` 在每个下游本地维护持久化幂等台账 `ledger-<name>.json`：

- 身份 = `sourceId/pointId#seq`，同一身份在该下游**最终只真正接收一次**；
- 台账在数据被接收时立即原子落盘，早于上游确认游标推进。因此
  "下游已成功接收、本地还没记下已确认就断电"后重启重放，这批数据命中台账判为 DUPLICATE，
  不会第二次真正接收（`acceptedTotal` 不增长，`duplicateDeliveries` 增长）；
- 转发**中途**崩溃（批次内部分身份已登记）重启后：已登记的判重、未登记的补收，
  最终仍是恰好一次（`CrashRecoveryIdempotencyTest` 用崩溃钩子模拟断电点）；
- 两个下游各自独立台账，互不影响。

### 幂等判定的边界语义（重要）

- 幂等判定**只看数据自身身份**，与投递次数、offset、先到后到无关：
  - **真正迟到的数据**（较小 seq，但该身份从未投递给这个下游）首次出现 → 照常 ACCEPTED，
    **不会**被误判成重复丢掉；
  - **真正的新数据**（更大 seq）→ ACCEPTED；
  - 只有"身份已在台账中"的再次投递才判 DUPLICATE。
- 采集侧的 `DedupGate` 乱序闸门与下游侧幂等台账是两道独立防线：前者在入口判定乱序/重发，
  后者保证转发/崩溃重放下的结果去重。测试中绕过入口闸门直送下游，专门验证后者边界。
- 这是**本地模拟**：真实系统里台账需要持久存储与（必要时）清理策略；本实现台账随数据单调增长，
  仅用于演示与本地验证。

## 背压与并发处置

- 采集侧写入只受缓存容量约束；多下游模式满时走确定性 `BUFFER_FULL` 拒绝路径，单出口模式走淘汰最旧路径，
  均有计数与 WARN，绝不静默丢弃。
- 每个下游一个单消费循环（批次 `telemetry.forward.batch-size`，间隔 `telemetry.forward.interval-millis`）：
  队首取批 → 发送 → 整批成功后推进游标。慢通道只拖慢自身。
- 通道 down 或发送失败/崩溃时不推进游标，数据保留待重试；
  可观测：各下游 `skippedDown`、`failedRounds`、`duplicateDeliveries`、`acceptedTotal`、`ackedOffset` 等。
- 对账不变式（测试断言）：
  - 单出口：`accepted == received + buffered + dropped`；
  - 多下游：`received(cloud)` 与 `received(archive)` 的身份集合/顺序/条数一致。

## 本地验证

### 单元测试（日志打印输入报文与关键判定过程）

```bash
mvn test
# 只看某个场景：
mvn test -Dtest=IngestServiceTest                 # 乱序/重复/迟到/拒绝原因（原有）
mvn test -Dtest=BufferOverflowTest                # 单出口缓存溢出与重启恢复（原有，兼容）
mvn test -Dtest=ChannelRecoveryTest               # 单出口通道抖动、恢复续传（原有，兼容）
mvn test -Dtest=BackpressureConcurrencyTest       # 并发采集 + 慢通道背压对账（原有）
mvn test -Dtest=DualDownstreamReconciliationTest  # 双下游一致性、隔离、重启各续各的、容量拒绝（新增）
mvn test -Dtest=CrashRecoveryIdempotencyTest      # 崩溃/中途崩溃后幂等去重、迟到新数据不误判（新增）
mvn test -Dtest=DecisionClassificationTest        # 每种判定可单独识别/统计/查询、合法数据不误判（新增）
```

### 运行期 REST 控制面

```bash
mvn spring-boot:run

# 注入报文（返回 decisionId 与 verdict/reason，输入与判定一一对应）
curl -X POST localhost:8080/api/ingest -H 'Content-Type: application/json' \
  -d '{"protocol":"kv","payload":"source=plc-1;point=temperature;seq=1;ts=1758000000000;value=23.5;quality=GOOD"}'

# 按 decisionId 单独查判定；列出最近判定
curl localhost:8080/api/decisions/d-000001
curl "localhost:8080/api/decisions?limit=20"

# 模拟某个下游抖动（downstream=cloud|archive）与恢复
curl -X POST "localhost:8080/api/channel/down?downstream=archive"
curl -X POST "localhost:8080/api/channel/up?downstream=archive"

# 手工触发某个下游转发一轮（不必启动后台循环），便于复现续传/判重
curl -X POST "localhost:8080/api/forward/flush?downstream=cloud"

# 启动/停止后台双下游转发循环，查看分下游可观测计数
curl -X POST localhost:8080/api/forwarder/start
curl localhost:8080/api/metrics
```

### 手工复现"崩溃续传不重复投递"

崩溃钩子在代码层提供（REST 不暴露断电模拟），见 `CrashRecoveryIdempotencyTest`：

1. 写入若干数据，对某下游设置 `channel.setCrashAfterLedgeredCount(k)`；
2. 调 `flushOnce(downstream)`，第 k 个身份登记后抛 `SimulatedCrash`（台账已落盘、游标未推进）；
3. 用**同一** buffer/ledger 目录重新 `new FileTelemetryBuffer(...)` + `new IdempotentSimulatedChannel(...)`
   + `new FanOutForwarder(...)` 模拟进程重启；
4. 再次 `flushOnce`：断言重放部分全部判重（`duplicateDeliveries`），`acceptedTotal` 不增加、
   台账身份集合恰好一次；中途崩溃用例验证"已登记判重 + 未登记补收"。
