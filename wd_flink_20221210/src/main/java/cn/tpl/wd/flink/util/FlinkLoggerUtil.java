package cn.tpl.wd.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author ChenXin
 * @create 2022-12-10-19:55
 */
public class FlinkLoggerUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkLoggerUtil.class);

    public static Logger getLogger() {
        try {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            if (stackTrace.length < 3 ||
                    !Objects.equals("getStackTrace", stackTrace[0].getMethodName()) ||
                    !Objects.equals("getLogger", stackTrace[1].getMethodName())){
                return LOGGER;
            }else {
                String className = stackTrace[2].getClassName();
                String[] split = className.split("\\.");
                return LoggerFactory.getLogger(split[split.length-1]);
            }
        }catch (Exception e){
            LOGGER.error("error occurred while getLogger()",e);
            return LOGGER;
        }
    }
}
