package com.github.lisama569.edgetelemetrygatewayjava.dedupe;

/**
 * 去重判定结果。previousSeq 为判定前该测点已接受的最大序号（无记录时为 -1），
 * 当后续持久化因背压失败时用于安全回滚本次预留。
 */
public record DedupeResult(DedupeDecision decision, long previousSeq) {
}
