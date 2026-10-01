package com.github.lisama569.edgetelemetrygatewayjava.telemetry.forward;

/**
 * 转发过程中的致命崩溃：数据可能已经到达下游，但本地在 ack 之前失去继续工作的能力
 * （如断电/进程被杀）。区别于普通可重试发送失败：
 * 调用方绝不能 ack 本批数据，且异常应向上传播而非被计数吞掉，由重启后续传处理。
 */
public class FatalForwardingCrash extends RuntimeException {

    public FatalForwardingCrash(String message) {
        super(message);
    }
}
