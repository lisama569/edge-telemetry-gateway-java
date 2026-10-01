package com.github.lisama569.edgetelemetrygatewayjava.telemetry.fanout;

import com.github.lisama569.edgetelemetrygatewayjava.telemetry.buffer.TelemetryBuffer;
import com.github.lisama569.edgetelemetrygatewayjava.telemetry.ingest.DecisionLog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/**
 * 多下游（云端出口 + 本地归档旁路）与判定日志的 Spring 装配。
 */
@Configuration
public class FanOutConfig {

    public static final String CLOUD = "cloud";
    public static final String ARCHIVE = "archive";

    @Bean
    public IdempotentSimulatedChannel cloudChannel(
            @Value("${telemetry.fanout.ledger-dir:./data/ledger}") Path ledgerDir) {
        return new IdempotentSimulatedChannel(CLOUD, ledgerDir);
    }

    @Bean
    public IdempotentSimulatedChannel archiveChannel(
            @Value("${telemetry.fanout.ledger-dir:./data/ledger}") Path ledgerDir) {
        return new IdempotentSimulatedChannel(ARCHIVE, ledgerDir);
    }

    @Bean
    public FanOutForwarder fanOutForwarder(
            TelemetryBuffer buffer,
            IdempotentSimulatedChannel cloudChannel,
            IdempotentSimulatedChannel archiveChannel,
            @Value("${telemetry.forward.batch-size:100}") int batchSize,
            @Value("${telemetry.forward.interval-millis:1000}") long intervalMillis) {
        return new FanOutForwarder(buffer,
                List.of(new Downstream(CLOUD, cloudChannel),
                        new Downstream(ARCHIVE, archiveChannel)),
                batchSize, intervalMillis);
    }

    @Bean
    public DecisionLog decisionLog(
            @Value("${telemetry.decision-log.capacity:1000}") int capacity) {
        return new DecisionLog(capacity);
    }
}
