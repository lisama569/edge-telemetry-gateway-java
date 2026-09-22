package com.github.lisama569.edgetelemetrygatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关配置项（edge.gateway.*）。
 */
@ConfigurationProperties(prefix = "edge.gateway")
public class GatewayProperties {

    private String dataDir = "data/gateway";
    private int cacheCapacity = 1000;
    private long offerTimeoutMs = 2000;
    private int fetchBatchSize = 50;
    private boolean fsync = true;
    /** 启动后是否暂停转发（仅测试/演练用：只缓存不投递）。 */
    private boolean startPaused = false;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getCacheCapacity() { return cacheCapacity; }
    public void setCacheCapacity(int cacheCapacity) { this.cacheCapacity = cacheCapacity; }
    public long getOfferTimeoutMs() { return offerTimeoutMs; }
    public void setOfferTimeoutMs(long offerTimeoutMs) { this.offerTimeoutMs = offerTimeoutMs; }
    public int getFetchBatchSize() { return fetchBatchSize; }
    public void setFetchBatchSize(int fetchBatchSize) { this.fetchBatchSize = fetchBatchSize; }
    public boolean isFsync() { return fsync; }
    public void setFsync(boolean fsync) { this.fsync = fsync; }
    public boolean isStartPaused() { return startPaused; }
    public void setStartPaused(boolean startPaused) { this.startPaused = startPaused; }
}
