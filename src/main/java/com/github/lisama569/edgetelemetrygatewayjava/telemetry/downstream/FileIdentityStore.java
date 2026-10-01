package com.github.lisama569.edgetelemetrygatewayjava.telemetry.downstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.PointIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地文件的身份账本（JSON Lines，临时文件 + 原子替换）。
 *
 * 模拟一个独立于网关进程、重启后仍保留的下游幂等账本：
 * - 每行一个 {@link PointIdentity}（sourceId/pointId/seq）；
 * - 水位由身份集合派生（每测点最大 seq），无需单独存储，天然一致；
 * - accept 后立即落盘：重启后“已真正接收”的身份不会丢，重复投递可被识别；
 * - 进程在转发中途崩溃（已交付但未 ack），重启后续传的同身份数据会被判为 DUPLICATE。
 */
public class FileIdentityStore implements IdentityStore {

    private static final Logger log = LoggerFactory.getLogger(FileIdentityStore.class);

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Set<PointIdentity> received = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();

    public FileIdentityStore(Path file) {
        this.file = file;
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init identity store at " + file, e);
        }
        log.info("identity store initialized: file={} recoveredIdentities={}", file, received.size());
    }

    private void load() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                registerLoaded(mapper.readValue(line, PointIdentity.class));
            }
        }
    }

    private void registerLoaded(PointIdentity identity) {
        received.add(identity);
        watermarks.merge(identity.pointKey(), identity.seq(), Math::max);
    }

    @Override
    public synchronized boolean contains(PointIdentity identity) {
        return received.contains(identity);
    }

    @Override
    public synchronized long watermark(String pointKey) {
        return watermarks.getOrDefault(pointKey, -1L);
    }

    @Override
    public synchronized void accept(PointIdentity identity) {
        if (received.add(identity)) {
            watermarks.merge(identity.pointKey(), identity.seq(), Math::max);
            persist();
            log.debug("identity accepted and persisted: {} (totalIdentities={})", identity, received.size());
        }
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            List<String> lines = new java.util.ArrayList<>(received.size());
            for (PointIdentity id : new HashSet<>(received)) {
                lines.add(mapper.writeValueAsString(id));
            }
            Files.write(tmp, lines, StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist identity store to " + file, e);
        }
    }
}
