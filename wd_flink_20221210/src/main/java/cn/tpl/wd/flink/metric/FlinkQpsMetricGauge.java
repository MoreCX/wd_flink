package cn.tpl.wd.flink.metric;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.metrics.Gauge;

/**
 * @author ChenXin
 * @create 2022-12-10-19:22
 */
public class FlinkQpsMetricGauge implements Gauge {
    private Cache<Long, Long> cache;

    public FlinkQpsMetricGauge() {
        CacheBuilder builder = CacheBuilder.newBuilder().maximumSize(5L);
        this.cache = builder.build();
    }

    @Override
    public Long getValue() {
        Long value = cache.getIfPresent(getPreviousWindow());
        return value == null ? 0L : value;
    }

    private long getPreviousWindow() {
        return System.currentTimeMillis() / 1000 - 1;
    }

    private long getCurrentWindow() {
        return System.currentTimeMillis() / 1000;
    }

    public void markEvent(long num) {
        long currentWindow = getCurrentWindow();
        Long value = cache.getIfPresent(currentWindow);
        cache.put(currentWindow, value == null ? num : num + value);
    }
}
