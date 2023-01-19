package cn.tpl.wd.flink.util;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author ChenXin
 * @create 2022-12-10-19:55
 */
public class FlinkRateLimiter {
    private static final Logger LOG = FlinkLoggerUtil.getLogger();

    private final RateLimiter rateLimiter;

    public FlinkRateLimiter(long queryPerSecond) {
        LOG.info("init rate limiter with thread:{}", Thread.currentThread().getName());
        this.rateLimiter = queryPerSecond > 0 ?
                RateLimiter.create(queryPerSecond, 30, TimeUnit.SECONDS) : null;
    }

    public void acquire() throws InterruptedException {
        if (this.rateLimiter == null) {
            return;
        }
        int tryCnt = 0;
        long startTm = System.currentTimeMillis();
        while (!rateLimiter.tryAcquire(1, 1, TimeUnit.SECONDS)) {
            tryCnt++;
            TimeUnit.SECONDS.sleep(1);
            if (tryCnt % 10 == 0) {
                long currentTm = System.currentTimeMillis();
                LOG.warn("can not acquire permit after 10 times,cost:{}ms", currentTm - startTm);
                startTm = currentTm;
            }
        }
    }
}
