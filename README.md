# edge-telemetry-gateway-java

边缘网关遥测采集与转发链路（Spring Boot）：在本地模拟多来源现场数据与两个云端通道，
验证乱序/重复/迟到报文的协议归一与去重、云端不可用时的本地缓存与恢复续传、
缓存溢出的确定处置、并发写入与背压下的多消费方顺序一致性。

详细设计、语义与本地验证方法见：[docs/EDGE_GATEWAY.md](docs/EDGE_GATEWAY.md)

## 快速开始

```bash
mvn test          # 全量测试（日志打印输入数据与关键判定过程）
mvn spring-boot:run
```

启动后：

```bash
curl -X POST localhost:8080/ingest/json -H 'Content-Type: text/plain' \
  --data-binary '{"sourceId":"s1","pointId":"temp","seq":1,"timestampMs":1727000000000,"value":23.5}'
curl localhost:8080/metrics
curl -X POST localhost:8080/simulate/burst
```
