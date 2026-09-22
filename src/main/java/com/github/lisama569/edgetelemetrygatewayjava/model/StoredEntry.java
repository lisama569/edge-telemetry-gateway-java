package com.github.lisama569.edgetelemetrygatewayjava.model;

/**
 * 本地缓存中的一条待转发记录。
 */
public record StoredEntry(
        long id,
        Reading reading
) {
}
