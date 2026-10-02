# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路的本地模拟实现：现场数据源与下游通道均由本地组件模拟，
采集、缓存、转发各环节在异常与恢复下的行为确定、可观测、结论可复现。

## 链路结构

默认双下游（云端出口 + 本地归档旁路），两个下游**共享一条准入日志、各自独立投递游标**：

```
                                   ┌─ 游标 cursor-cloud ─ 独立转发循环 ─> CloudDownstreamChannel
SimulatedSource ─RawFrame─> Ingest ─>   SharedFanOutBuffer            │
   (kv 报文)        (归一+去重判定)   └─ 游标 cursor-archive 独立转发循环 ─> ArchiveDownstreamChannel
                                   （同一条 fanout-log.jsonl，整条原子准入/拒绝）  (各自带幂等接收方)
```

`telemetry.archive.enabled=false` 时退化为上一版的单出口（仅 cloud，独立文件缓存，满则淘汰最旧）。

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

## 双下游旁路与一致性保证

### 保证的是哪种一致性

双下游对**任一个测点的任一个序号**保证：**两个下游要么最终都收到、要么都不收，
条数与顺序完全一致**。这是“共享准入 + 独立投递游标 + 独立幂等账本”实现的
**最终一致（eventual）、准入原子（all-or-nothing）**：

- 同一份采集侧已接受的数据由 `DownstreamFanOut` 追加进 `SharedFanOutBuffer` 的
  **同一条全局有序日志**（`fanout-log.jsonl`），两个下游只是在这条日志上持有各自独立的
  投递游标（`cursor-cloud.properties` / `cursor-archive.properties`）。不存在“先写一个缓存、
  再写另一个缓存”的中间态，因此不可能出现单侧入队失败造成的分叉。
- 每个下游各自 `peek 游标之后的批次 → 投递 → 推进自己的游标（ack）`，互不阻塞。
  全部追平后，两边按测点展开的序号序列逐条相同（`Reconciliation.diffByPoint` 零差异）。

### 单侧故障 / 变慢时

- 一个下游 down / 变慢 / 发送失败：**只停住它自己的游标与转发循环**，不拖另一个——
  快的一侧照样从共享日志里取它游标之后的数据并确认（`slowArchiveDoesNotSlowCloudDown`）。
- 慢/挂的一侧恢复（或进程重启）后，从**自己的游标**继续把欠的序号补齐；
  已确认（游标之前）的数据不会再次取出；若“已投递未 ack”的边界批次被再投一次，
  下游侧文件身份账本会判 **DUPLICATE**，同一身份只被真正接收一次。
- 恢复之后到达的全新合法序号正常 **ACCEPTED**，不会被误判成重复。

### 缓存写满时（关键，确定性处置）

- 容量按“**最旧一条尚未被所有下游确认**的日志条目”计数（共享容量，
  `telemetry.fanout.capacity`，与 `telemetry.buffer.capacity` 取较小值）。
- 写满时新数据被**整条拒绝**（`REJECTED_FULL`，计数 `rejectedFull` /
  `ingest.downstream.overflowRejected`，WARN 日志）：**两个下游一致地都不收这条**。
  绝不做“只淘汰某个下游缓存里的最旧数据”，因此不会再出现“一边照收、另一边悄悄丢更早数据、
  同一测点序号一边有一边没有”。
- 一旦慢侧追上、最旧条目被所有下游确认，共享日志压缩、容量立即腾出，后续新数据恢复准入。
- 取舍说明：写满期间新数据对**两侧都不可得**（确定性拒绝），这是为保证两侧严格一致所付的代价；
  好处是行为确定、可对账、不需要猜测哪条丢给了谁。

### 对账如实反映是否一致

- 单侧平衡不变式使用**真实准入计数**，而不是旧实现里
  `appended = received + buffered + dropped` 的反推（反推恒为真，永远发现不了问题）：
  `admitted == receivedUnique + buffered + evicted`，双下游共享路径 `evicted=0`，
  整条拒绝计在 `rejectedFull`。
- 跨下游一致性 `crossDownstreamDiffs(reference, other)` 比较每侧
  **已真正接收 + 仍在缓存（在途）** 的有序并集，逐测点对比身份序列——
  一侧还没 drain 完也能对账，分叉（缺序号/条数不同/顺序不同）立即体现在
  `crossDownstreamConsistent=false` 与 `cloudArchiveDiffs` 中。新增下游同样纳入对账
  （REST 端点按拓扑顺序逐对比较，不写死只有 cloud/archive）。

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

- **双下游模式（默认）**：`SharedFanOutBuffer` 落盘在 `./data/buffer/fanout/`：
  - `fanout-log.jsonl`：共享准入日志，每行 `(全局序号, TelemetryPoint)`，append 追加写、
    最旧条目被所有下游确认后整文件压缩重写（临时文件 + 原子替换）；
  - `cursor-cloud.properties` / `cursor-archive.properties`：各下游独立确认游标，
    ack 后立即原子落盘。重启后未确认窗口（游标之后）自动恢复，游标之前的数据不重发。
- **单出口兼容模式**：`FileTelemetryBuffer`（JSON Lines，默认 `./data/buffer/buffer.jsonl`），
  容量满确定性淘汰最旧并计数；`peekBatch` 只读，`ack(count)` 成功后从队首移除并落盘。

## 本地可观测：每种判定可单独识别、统计、查询

- 控制面 `/api/ingest` 对每条输入返回与之一一对应的 `verdict`：
  `ACCEPTED` / `DUPLICATE` / `LATE` / `REJECTED`（REJECTED 另带具体 `reason` 与 `detail`，
  LATE 带 `lastSeenSeq`），不用靠猜。
- 下游侧逐条判定为 `ACCEPTED` / `DUPLICATE` / `LATE`（`DeliveryResult`），
  计数分结论独立可查。
- `/api/metrics` 汇总采集计数（含 `downstream.overflowRejected`：两侧一致整条拒绝条数）
  与各下游计数；`/api/reconciliation` 给出分下游的**真实**对账数字
  （`admitted/receivedUnique/buffered/evicted/rejectedFull`）、`balanced` 判定、
  每对下游的 `pairDiffs`、汇总 `cloudArchiveDiffs` 以及布尔标志
  `crossDownstreamConsistent`（在途也对账，分叉时为 false）。
- 日志打印每条输入报文与关键判定过程（INFO 级别即可看到输入、采集 verdict、下游判重、
  共享缓存整条拒绝 WARN、各游标推进）。

## 对原单出口行为的兼容范围

- `telemetry.archive.enabled=false` 即恢复为仅 cloud 单出口拓扑：独立 `FileTelemetryBuffer`、
  满则**淘汰最旧**的旧语义完全保留（单出口下“宁可覆盖最旧也要继续收”的取舍不变）。
- 旧构造路径保留：`Forwarder(buffer, CloudChannel, ...)`、`IngestService(..., TelemetryBuffer)`、
  `SimulatedCloudChannel()` 无参构造、`FileTelemetryBuffer(dir, capacity)` 均未删除，
  原有测试（乱序重复、缓存溢出、通道恢复、并发背压、崩溃续传幂等等）仍全绿。
- 旧 REST 端点（`/api/channel/up|down`、`/api/forwarder/start|stop`）保留并作用于 cloud。
- 与上一版双下游行为的**差异（有意的修复）**：双下游不再“各缓存独立淘汰最旧”，
  改为共享日志 + 满则整条两侧一致拒绝；对账字段 `appended/dropped` 更名为语义更准确的
  `admitted/evicted` 并新增 `rejectedFull`，`balanced` 改为基于真实计数。
- cloud 通道按身份幂等去重的增强保留：只影响“同一身份重复投递”，正常数据投递结果不变。

## 本地验证

### 单元 / 集成测试（日志打印覆盖的业务场景与各用例结果）

```bash
mvn test
# 只看某个场景：
mvn test -Dtest=IngestServiceTest                 # 乱序/重复/迟到/拒绝原因（原有）
mvn test -Dtest=IngestVerdictClassificationTest   # 每种 verdict 一一对应、可统计
mvn test -Dtest=IdempotentReceiverTest            # 下游幂等：重投判重、真实迟到不被误判
mvn test -Dtest=CrashRecoveryIdempotencyTest      # 转发中途崩溃、重启续传不重复投递
mvn test -Dtest=DualDownstreamConsistencyTest     # 单侧故障/变慢不互拖、写满整条一致拒绝、
                                                  #   在途对账、恢复续传+新序号不被判重、独立游标
mvn test -Dtest=DualDownstreamBackpressureTest    # 并发采集+慢归档；停摆+写满+恢复后最终一致
mvn test -Dtest=SharedFanOutRestartTest           # 共享日志重启后续传、各游标补齐、ack 前崩溃幂等
mvn test -Dtest=ReconciliationHonestyTest         # 对账如实：反推恒平衡被废、单侧缺序号被暴露
mvn test -Dtest=BufferOverflowTest                # 缓存溢出与重启恢复（原有，单出口语义）
mvn test -Dtest=ChannelRecoveryTest               # 通道抖动、恢复续传（原有）
mvn test -Dtest=BackpressureConcurrencyTest       # 并发采集+慢通道背压对账（原有）
mvn test -Dtest=GatewayApiSmokeTest               # REST 控制面端到端冒烟
```

### 手工复现“一个下游挂掉/变慢 + 缓存写满 + 恢复续传”

```bash
# 用小容量启动，便于快速打满共享准入日志
mvn spring-boot:run -Dspring-boot.run.arguments=--telemetry.fanout.capacity=5

# 1) 归档侧停掉
curl -X POST localhost:8080/api/downstreams/archive/channel/down
# 2) 云端转发循环也先停掉，让 5 个槽位被占满（模拟最慢一侧完全不确认）
curl -X POST localhost:8080/api/downstreams/cloud/forwarder/stop
for i in $(seq 1 8); do
  curl -s -X POST localhost:8080/api/ingest -H 'Content-Type: application/json' \
    -d "{\"protocol\":\"kv\",\"payload\":\"source=plc-1;point=temperature;seq=$i;ts=1758000000000;value=$i;quality=GOOD\"}" >/dev/null
done
# 前 5 条准入，第 6~8 条在日志里 WARN "rejected for ALL downstreams"，两侧一致不收
curl -s localhost:8080/api/metrics | tr ',' '\n' | grep overflowRejected   # =3

# 3) 恢复云端并 flush：云端只收到 1..5（不会单侧收到 6..8）
curl -X POST localhost:8080/api/downstreams/cloud/forwarder/start
# 4) 恢复归档，让它按自己的游标补齐 1..5
curl -X POST localhost:8080/api/downstreams/archive/channel/up
sleep 2
# 5) 对账：两侧 balanced=true、crossDownstreamConsistent=true、cloudArchiveDiffs=[]、
#    rejectedFull=3（6,7,8 两边都没有；1..5 两边都有，顺序一致）
curl -s localhost:8080/api/reconciliation | python3 -m json.tool
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
# 分下游对账（balanced / admitted / receivedUnique / buffered / evicted / rejectedFull
# / 重复 / 迟到）+ crossDownstreamConsistent + 每对下游逐测点差异
curl localhost:8080/api/reconciliation
```

### 本地复现“崩溃续传不重复”

对应 `CrashRecoveryIdempotencyTest`：同一文件缓存 + 同一文件身份账本，
先用 `CrashBeforeAckChannel` 在“下游已接收、ack 前”抛 `GatewayCrashed`
（`FatalForwardingCrash`，转发器不 ack），再用同一对文件重建 buffer/接收方/转发器续传，
断言下游 `receivedUnique` 不增加、续传批次全部判 `DUPLICATE`，身份账本中每个身份恰好一条。
