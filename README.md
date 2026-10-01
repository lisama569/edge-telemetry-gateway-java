# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路的本地模拟实现：现场数据源与下游通道均由本地组件模拟，
采集、缓存、转发各环节在异常与恢复下的行为确定、可观测、结论可复现。

## 链路结构

默认双下游（云端出口 + 本地归档旁路），两个下游各自独立消费、独立进度：

```
                         ┌─> FileTelemetryBuffer(cloud)   ─ 独立转发循环 ─> CloudDownstreamChannel
SimulatedSource ─RawFrame─> IngestService ─TelemetryPoint─> DownstreamFanOut
   (kv 报文)        (归一+去重判定)         (同一份数据)   └─> FileTelemetryBuffer(archive) ─ 独立转发循环 ─> ArchiveDownstreamChannel
                                                                                                       (各自带幂等接收方)
```

`telemetry.archive.enabled=false` 时退化为上一版的单出口（仅 cloud）。

## 协议归一规则

- 统一模型 `TelemetryPoint(sourceId, pointId, seq, sampledAt, value, quality)`，
  测点由 `(sourceId, pointId)` 唯一标识，`seq` 为来源侧单调递增序号。
- 归一器按协议注册（`ProtocolNormalizer`），当前内置 `kv` 文本协议：
  `source=<id>;point=<id>;seq=<long>;ts=<epochMillis|ISO-8601>;value=<double>[;quality=GOOD|BAD|UNCERTAIN]`
- 拒绝原因可区分（`RejectReason`）：
  - `UNKNOWN_PROTOCOL`：协议类型未注册；
  - `MALFORMED_PAYLOAD`：结构非法（空报文、缺字段、字段无法解析）；
  - `INVALID_FIELD`：结构合法但取值非法（`seq<0`、`value` 非有限数、`quality` 未知）。
- 采集侧去重与乱序判定（`DedupGate`，按测点维护水位，线程安全）：
  - `seq > lastSeq` → **ACCEPTED**，推进水位并扇出；
  - `seq == lastSeq` 且内容一致 → **DUPLICATE**，丢弃；
  - `seq < lastSeq`，或 `seq == lastSeq` 但内容冲突 → **LATE**，绝不覆盖已确认数据。

## 双下游旁路与一致性对账

- 同一份已接收数据由 `DownstreamFanOut` 按固定顺序（先 cloud 后 archive）追加到
  **两个相互独立的文件缓存**；两边入队顺序一致，全部投递完成后每个测点的序号序列
  （顺序 + 条数）在两个下游逐条相同（`Reconciliation.diffByPoint` 为零差异）。
- **故障隔离**：每个下游拥有独立缓存、独立转发线程（`Forwarder`）与独立通道。
  一个下游 down / 变慢 / 发送失败：
  - 只阻塞它自己的消费循环，不拖另一个（慢归档不会让云端少收或延迟）；
  - 数据仍保留在它自己的缓存里，恢复后按它自己的进度续传；
  - 不会让另一个下游多收或漏收。
- **重复触发安全**：空队 `flushOnce()` 返回 0；已 `ack` 的数据不会被再次取出，
  重复触发 flush / 重启都不会重发已确认数据。
- **容量上限处置确定**：任一缓存满时确定性地淘汰该缓存最旧一条
  （`APPENDED_EVICTED_OLDEST` + `droppedCount` + WARN），只影响该下游自身。
  归档下游也纳入单下游对账：`appended == receivedUnique + buffered + dropped`。
- 双下游一致性是“最终收敛”语义：某一边暂时停摆时两边只在缓存条数上不同，
  各自 drain 完后按测点序列完全一致。

## 崩溃续传与幂等去重（边界语义）

针对“数据已发给下游成功、本地还没来得及记下已确认就断电重启，恢复时下游再收一次”的问题：

- 每个下游内置 `IdempotentReceiver`，按数据**自身身份**
  `PointIdentity = (sourceId, pointId, seq)` 判定，与值内容、投递次数无关。
  - 身份已接收过 → **DUPLICATE**：幂等丢弃，不真正接收（计数 `duplicateDeliveries`）；
  - 身份首次出现但 `seq < 该测点水位` → **LATE**：真实迟到数据，**仍真正接收**，仅打标
    （计数 `lateAccepted`），不会被误判成重复丢掉；
  - 身份首次出现且不落后 → **ACCEPTED**：正常新数据，真正接收。
- 已接收身份落在**独立于网关进程的文件账本**（`FileIdentityStore`，JSON Lines，
  目录 `telemetry.identity.dir`），accept 后立即原子落盘。因此：
  - 网关在转发中途崩溃（下游已收到、网关未 ack），重启后续传同批数据时，
    下游账本仍在 → 全部判 DUPLICATE，同一身份只被真正接收一次（effectively exactly-once）；
  - 续传后到达的全新身份正常 ACCEPTED，不会被误判。
- 转发循环对崩溃点用 `FatalForwardingCrash` 与普通发送失败区分：致命崩溃时**绝不 ack**
  并停止本轮调度，等价于进程死亡；普通失败仍可重试。
- **边界与前提（本地模拟）**：
  1. exactly-once 成立于“下游身份账本在网关重启后仍可用”这一模拟前提；账本自身丢失则退化为 at-least-once；
  2. 去重身份仅由 `(sourceId, pointId, seq)` 决定，不比较 value/时间戳，
     即“同身份不同内容”在下游仍按重复处理（采集侧 `DedupGate` 负责在入口拦截内容冲突的 Late）；
  3. 账本持久化是每条 accept 一次原子替换，未做跨批次合并/TTL 压缩，规模按本地验证量级设计；
  4. 崩溃模拟通过测试夹具 `CrashBeforeAckChannel`（下游真正收到后、ack 前抛
     `GatewayCrashed`）在本地复现，不涉及真实断电。

## 缓存与续传语义

- `FileTelemetryBuffer` 以 JSON Lines 落盘，云端默认 `./data/buffer/buffer.jsonl`，
  归档独立文件 `archive-buffer.jsonl`；每次变更先写临时文件再原子替换。
- `peekBatch` 只读，`ack(count)` 在该下游转发成功后从队首移除并立即落盘；
  未确认数据重启后自动恢复续传。

## 本地可观测：每种判定可单独识别、统计、查询

- 控制面 `/api/ingest` 对每条输入返回与之一一对应的 `verdict`：
  `ACCEPTED` / `DUPLICATE` / `LATE` / `REJECTED`（REJECTED 另带具体 `reason` 与 `detail`，
  LATE 带 `lastSeenSeq`），不用靠猜。
- 下游侧逐条判定为 `ACCEPTED` / `DUPLICATE` / `LATE`（`DeliveryResult`），
  计数分结论独立可查。
- `/api/metrics` 汇总采集计数与各下游计数；`/api/reconciliation` 给出分下游对账数字、
  `balanced` 判定，以及双下游逐测点差异 `cloudArchiveDiffs`。
- 日志打印每条输入报文与关键判定过程（INFO 级别即可看到输入、采集 verdict、下游判重）。

## 对原单出口行为的兼容范围

- `telemetry.archive.enabled=false` 即恢复为仅 cloud 单出口拓扑。
- 旧构造路径保留：`Forwarder(buffer, CloudChannel, ...)`、`IngestService(..., TelemetryBuffer)`、
  `SimulatedCloudChannel()` 无参构造、`FileTelemetryBuffer(dir, capacity)` 均未删除，
  原有四个测试类一行未改仍全绿。
- cloud 缓存路径/容量/溢出策略/批次与间隔语义不变；旧 REST 端点
  （`/api/channel/up|down`、`/api/forwarder/start|stop`）保留并作用于 cloud。
- 唯一行为增强：cloud 通道新增按身份幂等去重，只影响“同一身份重复投递”这一情形，
  正常数据投递结果不变。

## 本地验证

### 单元 / 集成测试（日志打印输入报文与关键判定）

```bash
mvn test
# 只看某个场景：
mvn test -Dtest=IngestServiceTest                 # 乱序/重复/迟到/拒绝原因（原有）
mvn test -Dtest=IngestVerdictClassificationTest   # 每种 verdict 一一对应、可统计
mvn test -Dtest=IdempotentReceiverTest            # 下游幂等：重投判重、真实迟到不被误判
mvn test -Dtest=CrashRecoveryIdempotencyTest      # 转发中途崩溃、重启续传不重复投递
mvn test -Dtest=DualDownstreamConsistencyTest     # 双下游一致性/故障隔离/溢出对账/独立进度
mvn test -Dtest=DualDownstreamBackpressureTest    # 并发采集+慢归档，双下游背压对账
mvn test -Dtest=BufferOverflowTest                # 缓存溢出与重启恢复（原有）
mvn test -Dtest=ChannelRecoveryTest               # 通道抖动、恢复续传（原有）
mvn test -Dtest=BackpressureConcurrencyTest       # 并发采集+慢通道背压对账（原有）
mvn test -Dtest=GatewayApiSmokeTest               # REST 控制面端到端冒烟
```

### 运行期 REST 控制面

```bash
mvn spring-boot:run

# 注入报文（返回 verdict + 具体原因，逐条对应）
curl -X POST localhost:8080/api/ingest -H 'Content-Type: application/json' \
  -d '{"protocol":"kv","payload":"source=plc-1;point=temperature;seq=1;ts=1758000000000;value=23.5;quality=GOOD"}'

# 按下游（cloud/archive）模拟抖动、恢复、手动续传
curl -X POST localhost:8080/api/downstreams/archive/channel/down
curl -X POST localhost:8080/api/downstreams/archive/channel/up
curl -X POST localhost:8080/api/downstreams/cloud/flush

# 启停后台转发循环（兼容旧端点：/api/forwarder/start 作用于 cloud）
curl -X POST localhost:8080/api/downstreams/cloud/forwarder/start
curl -X POST localhost:8080/api/downstreams/archive/forwarder/start

# 分结论计数 + 分下游计数
curl localhost:8080/api/metrics
# 分下游对账（balanced / appended / receivedUnique / buffered / dropped / 重复 / 迟到）+ 双边差异
curl localhost:8080/api/reconciliation
```

### 本地复现“崩溃续传不重复”

对应 `CrashRecoveryIdempotencyTest`：同一文件缓存 + 同一文件身份账本，
先用 `CrashBeforeAckChannel` 在“下游已接收、ack 前”抛 `GatewayCrashed`
（`FatalForwardingCrash`，转发器不 ack），再用同一对文件重建 buffer/接收方/转发器续传，
断言下游 `receivedUnique` 不增加、续传批次全部判 `DUPLICATE`，身份账本中每个身份恰好一条。
