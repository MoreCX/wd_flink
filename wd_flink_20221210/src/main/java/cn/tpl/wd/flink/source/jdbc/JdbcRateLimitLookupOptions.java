package cn.tpl.wd.flink.source.jdbc;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author ChenXin
 * @create 2022-12-10-19:52
 */
public class JdbcRateLimitLookupOptions implements Serializable {
    public static final ConfigOption<Long> LOOKUP_RATE_LIMIT =
            ConfigOptions.key("lookup.rate-limit.query-per-second")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Flag to rate limit. -1 by default");

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cacheMissingKey;
    private final long queryPerSecond;

    public JdbcRateLimitLookupOptions(
            long cacheMaxSize, long cacheExpireMs, int maxRetryTimes, boolean cacheMissingKey, long queryPerSecond) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.cacheMissingKey = cacheMissingKey;
        this.queryPerSecond = queryPerSecond;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public boolean getCacheMissingKey() {
        return cacheMissingKey;
    }

    public long getQueryPerSecond() {
        return queryPerSecond;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JdbcRateLimitLookupOptions) {
            JdbcRateLimitLookupOptions options = (JdbcRateLimitLookupOptions) o;
            return Objects.equals(cacheMaxSize, options.cacheMaxSize)
                    && Objects.equals(cacheExpireMs, options.cacheExpireMs)
                    && Objects.equals(maxRetryTimes, options.maxRetryTimes)
                    && Objects.equals(cacheMissingKey, options.cacheMissingKey)
                    && Objects.equals(queryPerSecond, options.queryPerSecond);

        } else {
            return false;
        }
    }
}
