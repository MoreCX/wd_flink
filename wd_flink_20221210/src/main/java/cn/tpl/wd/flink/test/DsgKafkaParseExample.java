package cn.tpl.wd.flink.test;

import cn.tpl.wd.flink.udf.DsgKafkaParseFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ChenXin
 * @create 2022-12-10-19:53
 */
public class DsgKafkaParseExample {
    private static final Logger logger = LoggerFactory.getLogger(EsSearchExample.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<EsSearchExample.SearchTable> dataStream = env.fromElements(new EsSearchExample.SearchTable(" {" +
                        "    \"T_PK\": \"123\"," +
                        "    \"T0\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T1\": \"zwose0H6IjpP5dt7Nnrl1JL94\"," +
                        "    \"T2\": \"2021-02-06 22:43:18\"," +
                        "    \"T3\": \"hbWAwZ0NXBcLta3vTJEu1kSGR\"," +
                        "    \"T4\": \"BzP9QlLXj0FGTZxmdVbYOurHD\"," +
                        "    \"T5\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T6\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T7\": \"2021-02-06 22:43:18\"," +
                        "    \"T8\": \"oPujx5Jh7fvRbqlAesM2Sg8FD\"," +
                        "    \"T9\": \"8IFQJAMXZqbf0mG\"," +
                        "    \"T10\": \"652\"," +
                        "    \"T11\": \"O5hpAwFu9Kc3TdPiBQVSXZHRa\"," +
                        "    \"T12\": \"pAZh1jDOLKiCTJWvkGmeqg0aI\"," +
                        "    \"T13\": \"uHzGPcrVYexyURW5M80wEX3AF\"," +
                        "    \"T14\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T15\": \"2021-02-06 22:43:18\"," +
                        "    \"T16\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T17\": \"xXgvO\"," +
                        "    \"T18\": \"pH9kn\"," +
                        "    \"T19\": \"TBQyxkrmKvfpuMaFqdWj0NJ2R\"" +
                        "  }", null, null),
                new EsSearchExample.SearchTable(null, " {" +
                        "    \"T_PK\": \"123\"," +
                        "    \"T0\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T1\": \"zwose0H6IjpP5dt7Nnrl1JL94\"," +
                        "    \"T2\": \"2021-02-06 22:43:18\"," +
                        "    \"T3\": \"hbWAwZ0NXBcLta3vTJEu1kSGR\"," +
                        "    \"T4\": \"BzP9QlLXj0FGTZxmdVbYOurHD\"," +
                        "    \"T5\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T6\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T7\": \"2021-02-06 22:43:18\"," +
                        "    \"T8\": \"oPujx5Jh7fvRbqlAesM2Sg8FD\"," +
                        "    \"T9\": \"8IFQJAMXZqbf0mG\"," +
                        "    \"T10\": \"652\"," +
                        "    \"T11\": \"O5hpAwFu9Kc3TdPiBQVSXZHRa\"," +
                        "    \"T12\": \"pAZh1jDOLKiCTJWvkGmeqg0aI\"," +
                        "    \"T13\": \"uHzGPcrVYexyURW5M80wEX3AF\"," +
                        "    \"T14\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T15\": \"2021-02-06 22:43:18\"," +
                        "    \"T16\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T17\": \"xXgvO\"," +
                        "    \"T18\": \"pH9kn\"," +
                        "    \"T19\": \"TBQyxkrmKvfpuMaFqdWj0NJ2R\"" +
                        "  }", " {" +
                        "    \"T_PK\": \"123\"," +
                        "    \"T0\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T1\": \"zwose0H6IjpP5dt7Nnrl1JL94\"," +
                        "    \"T2\": \"2021-02-06 22:43:18\"," +
                        "    \"T3\": \"hbWAwZ0NXBcLta3vTJEu1kSGR\"," +
                        "    \"T4\": \"BzP9QlLXj0FGTZxmdVbYOurHD\"," +
                        "    \"T5\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T6\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T7\": \"2021-02-06 22:43:18\"," +
                        "    \"T8\": \"oPujx5Jh7fvRbqlAesM2Sg8FD\"," +
                        "    \"T9\": \"8IFQJAMXZqbf0mG\"," +
                        "    \"T10\": \"652\"," +
                        "    \"T11\": \"O5hpAwFu9Kc3TdPiBQVSXZHRa\"," +
                        "    \"T12\": \"pAZh1jDOLKiCTJWvkGmeqg0aI\"," +
                        "    \"T13\": \"uHzGPcrVYexyURW5M80wEX3AF\"," +
                        "    \"T14\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T15\": \"2021-02-06 22:43:18\"," +
                        "    \"T16\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T17\": \"xXgvO\"," +
                        "    \"T18\": \"pH9kn\"," +
                        "    \"T19\": \"TBQyxkrmKvfpuMaFqdWj0NJ2R\"" +
                        "  }"),
                new EsSearchExample.SearchTable(null, null, " {" +
                        "    \"T_PK\": \"123\"," +
                        "    \"T0\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T1\": \"zwose0H6IjpP5dt7Nnrl1JL94\"," +
                        "    \"T2\": \"2021-02-06 22:43:18\"," +
                        "    \"T3\": \"hbWAwZ0NXBcLta3vTJEu1kSGR\"," +
                        "    \"T4\": \"BzP9QlLXj0FGTZxmdVbYOurHD\"," +
                        "    \"T5\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T6\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T7\": \"2021-02-06 22:43:18\"," +
                        "    \"T8\": \"oPujx5Jh7fvRbqlAesM2Sg8FD\"," +
                        "    \"T9\": \"8IFQJAMXZqbf0mG\"," +
                        "    \"T10\": \"652\"," +
                        "    \"T11\": \"O5hpAwFu9Kc3TdPiBQVSXZHRa\"," +
                        "    \"T12\": \"pAZh1jDOLKiCTJWvkGmeqg0aI\"," +
                        "    \"T13\": \"uHzGPcrVYexyURW5M80wEX3AF\"," +
                        "    \"T14\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T15\": \"2021-02-06 22:43:18\"," +
                        "    \"T16\": \"2021-02-06 22:43:18.000000000\"," +
                        "    \"T17\": \"xXgvO\"," +
                        "    \"T18\": \"pH9kn\"," +
                        "    \"T19\": \"TBQyxkrmKvfpuMaFqdWj0NJ2R\"" +
                        "  } "));
        Table searchTable = tableEnv.fromDataStream(dataStream);
        tableEnv.createTemporaryView("searchTable", searchTable);

        tableEnv.createFunction("dsg_kafka_parse", DsgKafkaParseFunction.class);
//        tableEnv.createFunction("get_json_object", JsonFunction.class);

        String sql = "select data, JSON_VALUE(data, 'T15') AS T15 " +
                " from searchTable " +
                " LEFT OUTER JOIN  " +
                " lateral table(dsg_kafka_parse(grouping_code, bus_model_code, stat_range)) as Prem(data) on true ";

        logger.info(sql);
        // 执行聚合统计查询转换
        Table eggResult = tableEnv.sqlQuery(sql);

        eggResult.printSchema();

        // table 转流输出,聚合统计是动态表，所以使用Changelog的方式才能输出
        tableEnv.toChangelogStream(eggResult).print("egg");
        env.execute();
    }

}
