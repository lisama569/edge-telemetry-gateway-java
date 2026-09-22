package com.github.lisama569.edgetelemetrygatewayjava.forward;

import com.github.lisama569.edgetelemetrygatewayjava.model.StoredEntry;

/**
 * 云端通道抽象（本地模拟实现）。
 * 云端必须按 idempotencyKey 幂等：重复续传不得重复计入。
 */
public interface CloudChannel {

    String name();

    /**
     * 投递一批数据；失败抛 ChannelException，由转发层重试。
     */
    void deliver(String idempotencyKey, java.util.List<StoredEntry> batch) throws ChannelException;

    boolean available();

    @SuppressWarnings("serial")
    class ChannelException extends Exception {
        public ChannelException(String msg) { super(msg); }
    }
}
