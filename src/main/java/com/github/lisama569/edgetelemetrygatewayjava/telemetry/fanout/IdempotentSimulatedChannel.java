package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.model.TelemetryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地模拟的幂等下游（云端出口 / 本地归档各一个实例）。
 *
 * 幂等语义（崩溃续传下不重复投递的结果保证）：
 * - 身份 identity = sourceId + "/" + pointId + "#" + seq；同一身份在该下游只被真正接收一次。
 * - 已成功接收的身份持久化到 ledger 文件；进程重启后从文件恢复，重放批次被判重复而不是再次接收。
 * - 幂等判定完全按数据自身身份，与来源投递次数/offset 无关：真正迟到的数据（seq 较小但从未投递过）
 *   与新数据（更大 seq）都是首次出现的身份，照常接收，不会被误判成重复。
 * - 通道 down 时 sendBatch 返回全部未处理（accepted=0），调用方不得推进游标。
 * - crash 钩子：模拟"下游已登记部分数据、本地尚未确认就断电"，第 N 次登记时抛出，
 *   重启后重放同一批次可验证已登记的判重、未登记的补收，最终 exactly-once。
 */
public class IdempotentSimulatedChannel {

    private static final Logger log = LoggerFactory.getLogger(IdempotentSimulatedChannel.class);

    /** 模拟"转发中途进程挂掉"：下游侧已幂等登记，但调用方还没来得及推进确认游标。 */
    public static class SimulatedCrash extends RuntimeException {
        public SimulatedCrash(String message) {
            super(message);
        }
    }

    public record SendResult(int acceptedCount, int duplicateCount) {
    }

    private final String name;
    private final Path ledgerFile;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Set<String> ledger = new LinkedHashSet<>();
    private final List<TelemetryPoint> received = new CopyOnWriteArrayList<>();
    private final AtomicLong acceptedTotal;
    private final AtomicLong duplicateTotal = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();

    private volatile boolean up = true;
    private volatile long latencyMillis = 0;
    /** >0 时，每发生一次"首次登记"就递减；降到 0 抛 SimulatedCrash（模拟断电）。 */
    private volatile int crashAfterLedgeredCount = 0;

    public IdempotentSimulatedChannel(String name, Path ledgerDir) {
        this.name = name;
        this.ledgerFile = ledgerDir.resolve("ledger-" + name + ".json");
        this.acceptedTotal = new AtomicLong();
        try {
            Files.createDirectories(ledgerDir);
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init ledger for " + name + " at " + ledgerDir, e);
        }
        acceptedTotal.set(ledger.size());
        log.info("idempotent downstream '{}' ready: ledger={} recoveredIdentities={}",
                name, ledgerFile, ledger.size());
    }

    private static String identityOf(TelemetryPoint p) {
        return p.sourceId() + "/" + p.pointId() + "#" + p.seq();
    }

    private void load() throws IOException {
        if (!Files.exists(ledgerFile)) {
            return;
        }
        List<String> recovered = mapper.readValue(
                Files.readString(ledgerFile, StandardCharsets.UTF_8), new TypeReference<List<String>>() {
                });
        ledger.addAll(recovered);
    }

    /**
     * 发送一批：逐个按身份幂等判定。返回本批真正接收数与判重数。
     * 通道 down 或中途 crash 时抛/返回失败语义，调用方不得按成功批次推进游标。
     */
    public synchronized SendResult sendBatch(List<TelemetryPoint> batch) {
        if (!up) {
            sendFailures.incrementAndGet();
            log.debug("downstream '{}' down, batch of {} rejected (cursor not advanced)", name, batch.size());
            return new SendResult(0, 0);
        }
        if (latencyMillis > 0) {
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendFailures.incrementAndGet();
                return new SendResult(0, 0);
            }
        }
        List<String> newlyLedgered = new ArrayList<>();
        List<TelemetryPoint> newlyReceived = new ArrayList<>();
        int duplicates = 0;
        for (TelemetryPoint p : batch) {
            String identity = identityOf(p);
            if (ledger.contains(identity)) {
                duplicates++;
                duplicateTotal.incrementAndGet();
                log.info("downstream '{}' verdict=DUPLICATE identity={} (already received, ignored)",
                        name, identity);
                continue;
            }
            ledger.add(identity);
            newlyLedgered.add(identity);
            newlyReceived.add(p);
            acceptedTotal.incrementAndGet();
            log.info("downstream '{}' verdict=ACCEPTED identity={} value={}", name, identity, p.value());

            if (crashAfterLedgeredCount > 0) {
                crashAfterLedgeredCount--;
                if (crashAfterLedgeredCount == 0) {
                    // 断电前把已登记身份落盘（这正是崩溃续传去重能够成立的关键），随后模拟进程终止
                    persistLedger();
                    received.addAll(newlyReceived);
                    log.warn("SIMULATED CRASH in downstream '{}' after ledgering {} identity/identities "
                            + "(caller has not acked)", name, newlyLedgered.size());
                    throw new SimulatedCrash("simulated power loss after ledgering into downstream '"
                            + name + "'");
                }
            }
        }
        persistLedger();
        received.addAll(newlyReceived);
        log.info("downstream '{}' batch processed: size={} accepted={} duplicate={} totalAccepted={}",
                name, batch.size(), newlyLedgered.size(), duplicates, acceptedTotal.get());
        return new SendResult(newlyLedgered.size(), duplicates);
    }

    /** 测试/对账用：某身份是否已在幂等台账中。 */
    public synchronized boolean isLedgered(long ignoredOffset, String identity) {
        return ledger.contains(identity);
    }

    /** 测试钩子：预登记一个身份（模拟下游侧历史已接收）。 */
    public synchronized void ledgerAccept(long offset, TelemetryPoint point) {
        String identity = identityOf(point);
        if (ledger.add(identity)) {
            acceptedTotal.incrementAndGet();
            persistLedger();
        }
    }

    private void persistLedger() {
        try {
            Path tmp = ledgerFile.resolveSibling(ledgerFile.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(new ArrayList<>(ledger)), StandardCharsets.UTF_8);
            Files.move(tmp, ledgerFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist ledger for " + name + " to " + ledgerFile, e);
        }
    }

    public boolean isUp() {
        return up;
    }

    public void bringUp() {
        up = true;
    }

    public void bringDown() {
        up = false;
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    /** 设为 N：接下来第 N 次首次身份登记后模拟崩溃（抛 SimulatedCrash）。&lt;=0 关闭。 */
    public void setCrashAfterLedgeredCount(int count) {
        this.crashAfterLedgeredCount = count;
    }

    public String name() {
        return name;
    }

    /** 该下游幂等台账中的全部身份（跨重启的"最终已被真正接收"集合），用于对账。 */
    public synchronized List<String> ledgeredIdentities() {
        return List.copyOf(ledger);
    }

    /** 该下游本次进程生命周期内真正接收（去重后）的数据，按接收顺序。 */
    public List<TelemetryPoint> received() {
        synchronized (this) {
            return List.copyOf(received);
        }
    }

    public long acceptedTotal() {
        return acceptedTotal.get();
    }

    public long duplicateTotal() {
        return duplicateTotal.get();
    }

    public long sendFailures() {
        return sendFailures.get();
    }
}
