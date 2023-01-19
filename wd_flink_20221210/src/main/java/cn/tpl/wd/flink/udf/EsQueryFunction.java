package cn.tpl.wd.flink.udf;

import cn.tpl.wd.flink.util.EsUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ChenXin
 * @create 2022-12-10-19:55
 */
public class EsQueryFunction extends TableFunction<String> {
    RestHighLevelClient client;
    private Logger logger = LoggerFactory.getLogger(EsQueryFunction.class);

    @Override
    public void open(FunctionContext context) throws Exception {
        client = EsUtils.createEsClient();
    }

    /**
     * @param str  参数， str[0]:index名称 ，str[1]:key名称，str[2]:value值  str[(length-1) % 2 = 1]key名称，str[(length-1) % 2 = 0]:value值
     * @throws IOException
     */
    public void eval(String ...str) {
        try {
            if (!validateArgs(str)) {
                collect("");
                return;
            }

            String index = str[0];
            ArrayList<String> arr = EsUtils.query(client, index, queryMap(str));
            for (String data : arr) {
                collect(data);
            }
        } catch (Exception e) {
            logger.error("query failed args string ", StringUtils.join(str, ","));
            logger.error("query failed ", e);
            collect("");
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }


    public boolean validateArgs(String ...str) {
        // 参数个数必须为 2整数倍+1 等等
        if((str.length - 1 ) % 2 != 0 )
            return false;
        return true;
    }

    public Map<String, Object> queryMap(String ...str) {
        Map<String, Object> map = new HashMap<>();
        String colName1 = "";
        String colData1 = "";
        //第一个元素为es index名称
        for(int i=1; i<str.length; i++) {
            if(i%2 == 1) {
                colName1 = str[i];
            } else {
                colData1 = str[i] == null ? "" : str[i];
                map.put(colName1, colData1);
            }
        }
        return map;
    }

}
