package cn.tpl.wd.flink.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ChenXin
 * @create 2022-12-10-19:54
 */
public class DsgKafkaParseFunction extends TableFunction<String> {
    private Logger logger = LoggerFactory.getLogger(DsgKafkaParseFunction.class);

    @Override
    public void open(FunctionContext context) throws Exception {
    }

    /**
     * @throws IOException
     */
    public void eval(String columnInfo, String after, String before) {
        try {
            JSONObject json;
            if(columnInfo != null) {
                json = JSON.parseObject(columnInfo);
                json.put("OP_FLAG", "insert");
//                logger.info("columnInfo" + json.toJSONString());
                collect(json.toJSONString());
            }

            if(after != null) {
                json = JSON.parseObject(after);
                json.put("OP_FLAG", "insert");
//                logger.info("after" + json.toJSONString());
                collect(json.toJSONString());
            }

            if(before != null) {
                json = JSON.parseObject(before);
                json.put("OP_FLAG", "delete");
//                logger.info("before" + json.toJSONString());
                collect(json.toJSONString());
            }
        } catch (Exception e) {
            logger.error("query failed args columnInfo： " , columnInfo);
            logger.error("query failed args after： " , after);
            logger.error("query failed args before： " , before);
            logger.error("query failed ", e);
            collect("{}");
        }
    }

    @Override
    public void close() throws Exception {
    }

}
