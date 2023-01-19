package cn.tpl.wd.flink;

import cn.tpl.wd.flink.util.FlinkRateLimiter;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;

/**
 * @author ChenXin
 * @create 2022-12-10-20:01
 */
public class FlinkRateLimiterTest {
    @Test
    public void test() throws InterruptedException {
        FlinkRateLimiter flinkRateLimiter = new FlinkRateLimiter(10L);
        TreeMap<Long, Integer> map = new TreeMap<>();
        for(int i=0;i<1000;i++){
            flinkRateLimiter.acquire();
            long key = System.currentTimeMillis()/1000;
            map.compute(key, (biKey, biVal) -> biVal == null ? 1 : biVal + 1);
        }
        System.out.println(map);
        map.values().forEach(v-> Assert.assertTrue(v<=10));
    }

}
