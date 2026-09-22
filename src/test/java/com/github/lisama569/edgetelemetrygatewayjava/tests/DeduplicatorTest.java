package com.github.lisama569.edgetelemetrygatewayjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.lisama569.edgetelemetrygatewayjava.dedupe.DedupeDecision;
import com.github.lisama569.edgetelemetrygatewayjava.dedupe.Deduplicator;
import com.github.lisama569.edgetelemetrygatewayjava.model.Reading;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 乱序、重复、迟到报文的判定测试：
 * - seq 相同 => DUPLICATE，不重复计数；
 * - seq 更小 => STALE，不覆盖新值；
 * - seq 更大 => ACCEPT；
 * - 不同测点互不影响；并发提交同一 seq 只有一个 ACCEPT。
 */
class DeduplicatorTest {

    private static final Logger log = LoggerFactory.getLogger(DeduplicatorTest.class);

    private Reading r(String source, String point, long seq) {
        return new Reading(source, point, seq, 1_727_000_000_000L + seq, seq + 0.0);
    }

    @Test
    void duplicateAndStaleAndOutOfOrder_areClassified() {
        Deduplicator dedupe = new Deduplicator();

        log.info("[测试输入] 按乱序到达 seq 序列: 2, 1(迟到旧报文), 2(重复), 3, 1(迟到)");
        assertEquals(DedupeDecision.ACCEPT, dedupe.decide(r("s1", "temp", 2)).decision());
        DedupeDecision stale = dedupe.decide(r("s1", "temp", 1)).decision();
        log.info("[关键判定] seq=1 -> {}", stale);
        assertEquals(DedupeDecision.STALE, stale);

        DedupeDecision duplicate = dedupe.decide(r("s1", "temp", 2)).decision();
        log.info("[关键判定] seq=2 -> {}", duplicate);
        assertEquals(DedupeDecision.DUPLICATE, duplicate);

        assertEquals(DedupeDecision.ACCEPT, dedupe.decide(r("s1", "temp", 3)).decision());
        assertEquals(DedupeDecision.STALE, dedupe.decide(r("s1", "temp", 1)).decision());

        // 新值未被旧值覆盖
        assertEquals(3L, dedupe.acceptedSequenceOf("s1", "temp"));
        log.info("[关键判定] 测点最终已接受最大序号={}（旧报文未覆盖新值）",
                dedupe.acceptedSequenceOf("s1", "temp"));

        // 不同测点独立计数
        log.info("[测试输入] 另一测点 seq=2 首次到达");
        assertEquals(DedupeDecision.ACCEPT, dedupe.decide(r("s1", "pressure", 2)).decision());
        assertEquals(2L, dedupe.acceptedSequenceOf("s1", "pressure"));
        assertEquals(3L, dedupe.acceptedSequenceOf("s1", "temp"));
    }

    @Test
    void concurrentSameSeq_onlyOneAccepted() throws InterruptedException {
        Deduplicator dedupe = new Deduplicator();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ConcurrentMap<DedupeDecision, Integer> counts = new ConcurrentHashMap<>();

        log.info("[测试输入] {} 个线程并发提交同一测点同一 seq=5", threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    DedupeDecision d = dedupe.decide(r("s1", "temp", 5)).decision();
                    counts.merge(d, 1, Integer::sum);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await(5, TimeUnit.SECONDS);
        pool.shutdownNow();

        log.info("[关键判定] 并发结论分布={}", counts);
        assertEquals(1, counts.getOrDefault(DedupeDecision.ACCEPT, 0));
        assertEquals(threads - 1, counts.getOrDefault(DedupeDecision.DUPLICATE, 0));
    }

    @Test
    void reserveReleasedAfterBackpressure_allowsRetryWithoutDuplicate() {
        Deduplicator dedupe = new Deduplicator();
        Reading five = r("s1", "temp", 5);

        var first = dedupe.decide(five);
        log.info("[测试输入] seq=5 首次判定={} previousSeq={}", first.decision(), first.previousSeq());
        assertEquals(DedupeDecision.ACCEPT, first.decision());

        // 模拟落盘背压失败 -> 回滚预留
        dedupe.release(five, first.previousSeq());
        log.info("[关键判定] 回滚后测点最大序号={}（应为 -1 表示无记录）",
                dedupe.acceptedSequenceOf("s1", "temp"));
        assertEquals(-1L, dedupe.acceptedSequenceOf("s1", "temp"));

        // 上游重试同一报文必须再次被接受，而不是误判 DUPLICATE
        var retry = dedupe.decide(five);
        log.info("[关键判定] 重试判定={}", retry.decision());
        assertEquals(DedupeDecision.ACCEPT, retry.decision());

        // 回滚不得覆盖并发已接受的更大序号
        Reading seven = r("s1", "temp", 7);
        var accepted7 = dedupe.decide(seven);
        assertEquals(DedupeDecision.ACCEPT, accepted7.decision());
        dedupe.release(five, first.previousSeq());
        assertEquals(7L, dedupe.acceptedSequenceOf("s1", "temp"));
    }
}
