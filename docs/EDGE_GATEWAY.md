# 边缘网关遥测采集与转发链路

本服务在本地模拟“多来源现场设备 → 边缘网关 → 两个云端通道”的完整链路，
用于验证在**报文乱序/重复/迟到、通道中断恢复、缓存溢出、并发与背压**
等条件下，系统的结论是否确定、可解释、可复现。

```
现场数据源(模拟 JSON/CSV)
        │  POST /ingest/{protocol}
        ▼
 IngestService（协议归一 → 乱序/重复判定 → 背压落盘）
        │
        ▼
 PendingStore（本地 WAL 缓存 + 每消费方水位 + 压缩）
        │              │
        ▼              ▼
 primary 云端       archive 云端        （每通道单线程 FIFO worker）
```

---

## 1. 协议归一规则

所有协议解析为统一记录：

```json
{"sourceId":"sensor-A","pointId":"temperature","seq":2,
 "timestampMs":1727000000001,"value":23.4}
```

- `seq`：现场数据源对该 `(sourceId, pointId)` 测点单调递增的序号，
  是乱序/重复判定的唯一依据（与到达时间无关）。
- 当前支持两种协议：
  - `json`：标准 JSON 对象，字段固定，多余字段忽略；
  - `csv`：5 列 `sourceId,pointId,seq,timestampMs,value`。

无法识别的报文返回 `outcome=REJECTED`，`rejectReason` 可区分：

| rejectReason | 含义 |
|---|---|
| `UNKNOWN_PROTOCOL` | 协议名未注册（非 json/csv） |
| `MALFORMED` | JSON 语法错误/根节点非对象；CSV 列数不等于 5；空报文 |
| `MISSING_FIELD` | 必填字段缺失、为 null 或空白字符串 |
| `BAD_VALUE` | 字段类型错误、seq/timestampMs 为负、value 为 NaN/Infinity、数字不可解析 |

同测点报文的时序判定（`Deduplicator`，按测点原子更新）：

| 条件 | 结论 | 计数 |
|---|---|---|
| `seq` 大于已接受最大序号 | `ACCEPTED`（预留序号→落盘→提交） | accepted/persisted |
| `seq` 等于已接受最大序号 | `DUPLICATE`，不重复计数 | duplicate |
| `seq` 小于已接受最大序号 | `STALE`（迟到旧报文），**不允许覆盖新值** | stale |

预留与提交分离：若 `ACCEPTED` 之后落盘因背压失败，会**回滚序号预留**，
保证上游重试同一报文时仍被接受，而不是被误判为重复而静默丢失。

---

## 2. 缓存与续传语义

数据目录（`edge.gateway.data-dir`，默认 `data/gateway`）包含：

- `wal.log`：只追加日志，每行一条 `{"id":...,"r":Reading}`，写入后 `force` 刷盘；
- `offset-{consumer}.wm`：每个消费方独立的水位文件（原子替换写盘）。

核心规则：

1. **entryId 全局单调递增**，重启后从 WAL 最大 id 续号。
2. **每消费方独立水位**：`fetch(consumer)` 永远只取该消费方水位之后的批次；
   `confirm(consumer,id)` 单调推进水位并立即落盘。
   - 已确认数据重启后不重发；
   - 未确认数据重启后从 WAL 恢复，不丢失。
3. **容量以最慢消费方为准**：缓存占用 = entryId 大于“所有已注册消费方最低水位”
   的条目数。只要还有任一消费方没确认，数据就占容量——慢通道长期不确认，
   背压最终会传导回采集侧，慢消费方不会落后无限远。
4. **WAL 压缩安全边界**：仅当所有已注册消费方都确认过某个前缀
   （entryId ≤ 最低水位）才物理删除；系统中存在未确认消费方时（水位 0）
   绝不物理删数。
5. **云端幂等**：每批使用确定性幂等键 `consumer:firstId-lastId`。
   整批重发（失败重试、重启续传）由云端按幂等键去重，已确认数据不重复计数。

容量满时的处置是**确定且可预期**的：

- 采集线程在缓存满时阻塞等待空间（不丢数据）；
- 等待超过 `edge.gateway.offer-timeout-ms` 仍无空间，返回
  `outcome=BACKPRESSURE_REJECTED, reason=BACKPRESSURE_REJECTED`，
  同时 `metrics.backpressureRejected +1`；
- 这是**显式、可观测、可由上游重试**的拒绝，不是静默丢弃；
  通道恢复、缓存排空后重试同报文可成功写入。

---

## 3. 转发并发与背压处置

- 每个云端通道一个**专属单线程 worker**（虚拟线程）。同一消费方严格
  按 entryId FIFO 投递，因此多个消费方看到的顺序与内容结论在任何抖动、
  失败、重启下都一致、可复现。
- 通道不可用：worker 周期性探测（200ms 退避），期间数据只进本地缓存；
  通道恢复或调用 `kick()` 后立即从各自水位续传。
- 投递抛错：`retryAttempts +1`，整批稍后重发，云端幂等保证不重复。
- 慢通道（可设置投递延迟）只阻塞自己的 worker，不影响另一通道和采集；
  容量占满后才通过显式背压反压采集侧。
- 可观测指标（`GET /metrics`）：received/accepted/rejected/duplicate/stale/
  backpressureRejected/persisted/deliveredPrimary/deliveredArchive/
  retryAttempts/channelFailures，以及 cachePending/cacheCapacity/lastEntryId。
- `GET /forward/status` 给出每个通道的可用性与当前水位，
  是“通道是否健康、积压多深”的直接判定依据。

转发可暂停（`edge.gateway.start-paused=true` 或 `ForwardingService.pause()`），
暂停期间只缓存不投递，用于演练恢复时刻；`resume()` 后继续。

---

## 4. HTTP 接口（本地验证）

| 方法与路径 | 作用 |
|---|---|
| `POST /ingest/{protocol}` | 投递一条原始报文，body 为报文原文（建议 `Content-Type: text/plain`） |
| `GET /metrics` | 全链路计数 + 缓存占用/容量/lastEntryId |
| `GET /forward/status` | 各通道可用性与水位 |
| `POST /simulate/burst` | 发送一轮确定性报文（含重复/乱序/迟到/非法） |
| `POST /channels/{name}/availability?available=` | 模拟通道断连/恢复（name=primary/archive） |
| `POST /channels/{name}/fail-next?n=2` | 让接下来 n 次投递确定性失败 |
| `POST /channels/{name}/delay?ms=500` | 设置投递延迟，模拟慢通道 |

IngestResult 响应字段：`outcome`（ACCEPTED/DUPLICATE/STALE/REJECTED/
BACKPRESSURE_REJECTED）、`rejectReason`、`entryId`、`detail`。

---

## 5. 本地验证方法

### 5.1 运行测试（推荐，日志会打印输入数据与关键判定）

```bash
mvn test
```

测试覆盖：

| 测试类 | 覆盖点 |
|---|---|
| `NormalizerTest` | JSON/CSV 归一；MALFORMED/MISSING_FIELD/BAD_VALUE 拒绝原因可区分 |
| `DeduplicatorTest` | 乱序/重复/迟到判定；并发同序号仅一个 ACCEPT；背压预留回滚可重试 |
| `PendingStoreTest` | 多消费方独立水位、压缩、容量满阻塞+超时拒绝、**重启 WAL/水位续传不重不丢** |
| `GatewayEndToEndTest` | 端到端：乱序重复非法报文分类；主通道中断缓存+恢复续传；失败重试幂等；两消费方顺序一致 |
| `BackpressureIntegrationTest` | 容量满显式背压拒绝（计数可观测、非静默丢弃）+ 恢复后重试成功 |
| `ConcurrentIngestTest` | 8 线程并发写入 + 通道抖动：entryId 连续无空洞、两云端 FIFO 完全一致 |
| `RestartResumeIntegrationTest` | 两个 Spring 上下文模拟进程重启：未确认数据续传、已确认数据不重发 |

查看带输入与判定过程的日志：

```bash
mvn test -Dtest=GatewayEndToEndTest
cat target/surefire-reports/TEST-*GatewayEndToEndTest.xml | less
```

### 5.2 手动冒烟

```bash
mvn spring-boot:run
# 或：mvn package && java -jar target/edge-telemetry-gateway-java-1.0-SNAPSHOT.jar

curl localhost:8080/metrics
curl -X POST localhost:8080/ingest/json -H 'Content-Type: text/plain' \
  --data-binary '{"sourceId":"s1","pointId":"temp","seq":1,"timestampMs":1727000000000,"value":23.5}'
curl -X POST localhost:8080/ingest/csv -H 'Content-Type: text/plain' \
  --data-binary 's2,p,7,1727000000007,1.5'
curl -X POST 'localhost:8080/channels/primary/availability?available=false'
curl -X POST 'localhost:8080/channels/primary/availability?available=true'
curl localhost:8080/forward/status
curl -X POST localhost:8080/simulate/burst
```

---

## 6. 配置项（`edge.gateway.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `data/gateway` | WAL 与水位文件目录 |
| `cache-capacity` | `1000` | 待全部消费方确认的缓存条目上限 |
| `offer-timeout-ms` | `2000` | 缓存满时采集侧最长等待，超时显式背压拒绝 |
| `fetch-batch-size` | `50` | 单批转发最大条数（FIFO） |
| `fsync` | `true` | WAL/水位写入是否 force 刷盘 |
| `start-paused` | `false` | 启动后暂停转发（只缓存不投递，用于演练） |
