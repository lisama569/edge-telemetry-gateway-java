package com.github.lisama569.edgetelemetrygatewayjava.telemetry;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 控制面冒烟：注入报文返回可区分 verdict；双下游各自可 flush；
 * 对账与 metrics 可查询且平衡。使用独立 data 目录避免污染默认目录。
 */
@SpringBootTest(properties = {
        "telemetry.buffer.dir=target/smoke-data/buffer",
        "telemetry.identity.dir=target/smoke-data/identities",
        "telemetry.buffer.capacity=1000",
        "telemetry.archive.buffer.capacity=1000"
})
@AutoConfigureMockMvc
class GatewayApiSmokeTest {

    @BeforeAll
    static void cleanData() throws IOException {
        deleteRecursively(Path.of("target/smoke-data"));
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Autowired
    private MockMvc mvc;

    private String kv(long seq) {
        return "{\"protocol\":\"kv\",\"payload\":\"source=plc-1;point=smoke;seq=" + seq
                + ";ts=1758000000000;value=" + seq + ";quality=GOOD\"}";
    }

    @Test
    void ingestVerdictsFlushBothDownstreamsAndReconcile() throws Exception {
        mvc.perform(post("/api/ingest").contentType("application/json").content(kv(1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.verdict").value("ACCEPTED"))
                .andExpect(jsonPath("$.seq").value(1));
        mvc.perform(post("/api/ingest").contentType("application/json").content(kv(1)))
                .andExpect(jsonPath("$.verdict").value("DUPLICATE"));
        mvc.perform(post("/api/ingest").contentType("application/json")
                        .content("{\"protocol\":\"kv\",\"payload\":\"source=plc-1;point=smoke\"}"))
                .andExpect(jsonPath("$.verdict").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("MALFORMED_PAYLOAD"));

        mvc.perform(post("/api/downstreams/cloud/flush")).andExpect(status().isOk())
                .andExpect(jsonPath("$.confirmed").value(1));
        mvc.perform(post("/api/downstreams/archive/flush")).andExpect(status().isOk())
                .andExpect(jsonPath("$.confirmed").value(1));

        mvc.perform(get("/api/reconciliation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cloud.balanced").value(true))
                .andExpect(jsonPath("$.archive.balanced").value(true))
                .andExpect(jsonPath("$.cloud.receivedUnique").value(1))
                .andExpect(jsonPath("$.archive.receivedUnique").value(1))
                .andExpect(jsonPath("$.cloudArchiveDiffs").isArray());

        mvc.perform(get("/api/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['ingest.accepted']").value(1))
                .andExpect(jsonPath("$['ingest.duplicates']").value(1))
                .andExpect(jsonPath("$.downstreams.cloud").exists())
                .andExpect(jsonPath("$.downstreams.archive").exists());
    }
}
