# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路的本地模拟实现：现场数据源与下游通道均由本地组件模拟，
采集、缓存、转发各环节在异常与恢复下的行为确定、可观测、结论可复现。

## 链路结构

默认双下游（云端出口 + 本地归档旁路），两个下游**共享同一条持久化队列**、各自独立确认游标：

```
                         ┌─ 确认游标 cloud ─ 独立转发循环 ─> CloudDownstreamChannel
SimulatedSource ─RawFrame─> IngestService ─TelemetryPoint─> FileSharedFanOutBuffer(同一条队列)
   (kv 报文)        (归一+去重判定)         (只入队一次)   └─ 确认游标 archive ─ 独立转发循环 ─> ArchiveDownstreamChannel
                                                                                    (各自带幂等接收方)
```

同一份数据只在共享队列入队一次，cloud/archive 看到的是**同一条入队序列**；队首由最慢的下游
决定，只有两边都确认过的条目才会裁剪。`telemetry.archive.enabled=false` 时退化为上一版的单出口
（仅 cloud，独占一个“满则淘汰最旧”的独立缓存）。

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

### 保证哪种一致性

- **同一份数据只入队一次**：`DownstreamFanOut` 把已接收数据写入一条共享持久化队列
  `FileSharedFanOutBuffer`，cloud 与 archive 通过各自的确认游标（`BufferEndpoint`）消费
  **同一条入队序列**。因此对任意测点的任意序号，两个下游最终**要么都收到、要么都不收到**，
  条数与顺序一致（全部确认后 `Reconciliation.diffByPoint` 为零差异）。
- **队首由最慢下游决定**：一个条目只有在**所有**下游都确认后才会从共享队列裁剪。
  某个下游 down / 变慢 / 发送失败时，属于它的未确认条目始终留在队列里等它恢复，
  不会被快的一侧“带走”，也不会被淘汰——从机制上杜绝了“恢复侧缺序号/丢更早数据”。
- **故障隔离仍然成立**：每个下游有独立通道、独立幂等账本、独立转发线程（`Forwarder`）。
  慢/停的一侧只会让共享队列堆积（直到写满产生背压），**正常一侧不等待、不被拖慢**，
  它仍按自己的游标持续确认；只是在最慢一侧追上之前，已被大家确认之前的队列空间不会释放。

### 缓存写满时的确定性处置（不再静默淘汰）

- 共享容量指“仍至少被一个下游占用（未被全部确认）”的最大条数。
- 写满时 `FileSharedFanOutBuffer.append` **确定性拒绝本次写入**
  （`REJECTED_FULL` + `rejectedFullCount` + WARN），**不入队、不淘汰任何旧数据**。
- 采集侧把它判为 `REJECTED / BUFFER_FULL`，且**不推进去重水位**：调用方退避重试时，
  同一报文仍是合法新数据，不会被误判成 `DUPLICATE` 而永久丢失。
- 因此写满绝不会造成“一边照收、另一边悄悄淘汰更早数据”的两侧分叉；
  被拒数据在最慢下游追上、容量释放后重试补齐即可。
- **重复触发安全**：空队 `flushOnce()` 返回 0；已 `ack` 的游标不会回退，
  重复 flush / 重启都不会重发已确认数据；崩溃在 ack 前则靠下游身份账本去重（见下节）。
- **单出口（兼容模式）保留旧语义**：`FileTelemetryBuffer` 满时仍确定性淘汰最旧并计 dropped，
  与历史行为一致；淘汰策略只在“单一下游”这一不存在双边一致性约束的场景保留。

### 恢复续传与重启

- 慢/停侧恢复后，各自由自己的游标续传欠下的数据；全部补齐后逐测点序号序列完全一致。
- 共享队列（`fanout/fanout-queue.jsonl`）与各下游游标（`fanout/fanout-cursors.jsonl`）落盘，
  进程重启后：已确认条目不重发，未确认条目按各自游标恢复续传（at-least-once，配合下游幂等
  账本实现 effectively exactly-once）。

### 对账如何如实反映一致性

- 单下游平衡恒等式使用**真实追加计数**：`appended == receivedUnique + pending(+ dropped)`，
  `appended` 来自共享队列 `appendedTotal`/采集 accepted，**不能由右侧反算**（旧实现反算导致
  永远平衡、掩盖丢数据）。
- 双下游结论 `Reconciliation.reconcilePair` 明确区分三种状态：
  - `CONSISTENT`：两边在途均为 0 且逐测点序列一致；
  - `IN_FLIGHT`：至少一边仍有在途（慢/停侧追赶中），暂未对齐属正常，不算不一致；
  - `DIVERGED`：两边在途都已清空却仍逐测点对不上——**真正不一致，必须暴露**。
- 新增下游通过 `subscribe` 纳入同一条共享序列与对账；不允许在有在途数据时中途加入新下游，
  以免凭空制造缺口（`subscribe` 会拒绝这种装配）。

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

- 双下游：`FileSharedFanOutBuffer` 落盘为单张快照 `telemetry.buffer.dir/fanout/fanout-snapshot.jsonl`：
  - 首行单调计数（globalTrimIndex/appendedTotal/rejectedFull），其后每下游一行确认游标，再其后为队列条目；
  - 计数、游标、队列在同一张快照里、一次原子替换落盘，因此任意一次入队/确认/裁剪要么整体生效、
    要么整体不生效，进程在任意时刻被杀都能恢复到上一个一致快照；
  - `peekBatch` 只读，`ack(count)` 仅推进本下游游标，所有下游都确认后才裁剪队首；
    重启自动恢复队列与各自游标。
- 单出口（兼容模式）：`FileTelemetryBuffer` 以 JSON Lines 落盘于
  `telemetry.buffer.dir/buffer.jsonl`，语义保持满则淘汰最旧。

## 本地可观测：每种判定可单独识别、统计、查询

- 控制面 `/api/ingest` 对每条输入返回与之一一对应的 `verdict`：
  `ACCEPTED` / `DUPLICATE` / `LATE` / `REJECTED`（REJECTED 另带具体 `reason` 与 `detail`，
  LATE 带 `lastSeenSeq`），不用靠猜。
- 下游侧逐条判定为 `ACCEPTED` / `DUPLICATE` / `LATE`（`DeliveryResult`），
  计数分结论独立可查；采集侧缓存满新增可区分拒绝原因 `BUFFER_FULL`。
- `/api/metrics` 汇总采集计数与各下游计数；`/api/reconciliation` 给出：
  - 分下游 `appended / receivedUnique / pending / dropped / balanced`（真实计数口径）；
  - `cloudArchive.status`：`CONSISTENT` / `IN_FLIGHT` / `DIVERGED`，以及 `cloudPending`、
    `archivePending`、逐测点 `diffs`、共享队列 `appendedTotal`、`rejectedFull`。
- 日志打印每条输入报文与关键判定过程（INFO 级别即可看到输入、采集 verdict、下游判重、背压拒绝）。

## 对原单出口行为的兼容范围

- `telemetry.archive.enabled=false` 即恢复为仅 cloud 单出口拓扑：使用独立 `FileTelemetryBuffer`，
  满则淘汰最旧；容量/批次/间隔语义、旧 REST 端点（`/api/channel/up|down`、
  `/api/forwarder/start|stop`，作用于 cloud）均保留不变。
- 旧构造路径保留：`Forwarder(buffer, CloudChannel, ...)`、`IngestService(..., TelemetryBuffer)`、
  `SimulatedCloudChannel()` 无参构造、`FileTelemetryBuffer(dir, capacity)` 均未删除，
  原有乱序/重复、缓存溢出、通道恢复、崩溃续传、并发背压等测试全部保留且仍全绿。
- 双下游默认路径的**唯一语义变更**（修复缺陷）：由“两条各自满则淘汰最旧的独立缓存”改为
  “一条共享队列 + 独立确认游标 + 满则背压拒绝”，因此单侧故障叠加写满不再造成两侧永久分叉。
  对正常无故障数据流，投递结果、顺序、幂等去重行为与之前一致。

## 本地验证

### 单元 / 集成测试（日志打印输入报文与关键判定）

```bash
mvn test
# 只看某个场景：
mvn test -Dtest=IngestServiceTest                 # 乱序/重复/迟到/拒绝原因（原有）
mvn test -Dtest=IngestVerdictClassificationTest   # 每种 verdict 一一对应、可统计
mvn test -Dtest=IdempotentReceiverTest            # 下游幂等：重投判重、真实迟到不被误判
mvn test -Dtest=CrashRecoveryIdempotencyTest      # 单出口转发中途崩溃、重启续传不重复投递
mvn test -Dtest=SharedFanOutRestartTest           # 共享队列重启恢复各下游游标、ack前崩溃不重发
mvn test -Dtest=DualDownstreamConsistencyTest     # 双下游一致性/故障隔离/写满背压/独立进度/重试不判重
mvn test -Dtest=DualDownstreamBackpressureTest    # 并发采集+慢归档、停摆写满+恢复补齐最终一致
mvn test -Dtest=ReconciliationHonestyTest         # 对账区分 CONSISTENT/IN_FLIGHT/DIVERGED，真实计数
mvn test -Dtest=BackpressureRetryNotDuplicateTest # 缓存满背压后重试合法数据不被误判成重复
mvn test -Dtest=BufferOverflowTest                # 单出口缓存溢出与重启恢复（原有）
mvn test -Dtest=ChannelRecoveryTest               # 单出口通道抖动、恢复续传（原有）
mvn test -Dtest=BackpressureConcurrencyTest       # 单出口并发采集+慢通道背压对账（原有）
mvn test -Dtest=GatewayApiSmokeTest               # REST 控制面端到端冒烟
```

### 本地复现“单侧故障 + 写满 + 恢复一致”

对应 `DualDownstreamBackpressureTest#stoppedArchiveFillsSharedBufferThenRejectedWritesAreRetriedAfterRecovery`
（确定性、无需真实断电）：

1. 建一个容量 300 的共享队列，订阅 cloud/archive；
2. `archive.bringDown()` 模拟归档停摆，云端后台正常消费；4 个并发测点各产 200 条（共 800）；
3. 观察到：入队最多 300 条、`rejectedFull>0`，队列满载且**不含任何淘汰**，云端只收到入队的前缀，
   archive 收到 0 条；共享队列 `size()==appendedTotal`（archive 0 确认，队首不裁剪）；
4. `archive.bringUp()`，两下游把已入队部分 drain 空，`diffByPoint` 为空、云端无重复；
5. 把写满时未入队的序号作为合法数据重试（此时容量持续释放），最终两边各 800 条、
   每测点序号 1..200 恰好一条，`diffByPoint` 为空、双端 `duplicateCount()==0`。

也可用 REST 控制面手工复现：

```bash
mvn spring-boot:run
# 归档停摆，持续注入超过容量(默认10000)的报文，观察 /api/reconciliation：
#   cloud.archive.status=IN_FLIGHT，archivePending 持续增长，rejectedFull>0 时采集返回 BUFFER_FULL
curl -X POST localhost:8080/api/downstreams/archive/channel/down
# 恢复后触发/等待续传，最终 status=CONSISTENT、diffs=[]
curl -X POST localhost:8080/api/downstreams/archive/channel/up
curl localhost:8080/api/reconciliation
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
