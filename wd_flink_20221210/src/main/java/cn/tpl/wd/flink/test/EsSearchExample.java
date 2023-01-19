package cn.tpl.wd.flink.test;

import cn.tpl.wd.flink.udf.EsQueryFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ChenXin
 * @create 2022-12-10-19:54
 */
public class EsSearchExample {
    private static final Logger logger = LoggerFactory.getLogger(EsSearchExample.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<SearchTable> dataStream = env.fromElements(new SearchTable("1000000000", "qb", "Year"),
                new SearchTable("1000000000", "oto", "Year"),
                new SearchTable("1000000000", "ct", "Year"),
                new SearchTable("1000000000", "qt", "Year"),
                new SearchTable("1000000000", "wf", "Year"));
        Table searchTable = tableEnv.fromDataStream(dataStream);
        tableEnv.createTemporaryView("searchTable", searchTable);

        tableEnv.createFunction("es_search_object", EsQueryFunction.class);
//        tableEnv.createFunction("get_json_object", JsonFunction.class);

        String sql = "select grouping_code" +
                " ,bus_model_code" +
                " ,stat_range " +
                " ,round(cast(JSON_VALUE(data, 'prem') as double) / 10000, 2) as prem " +
                " ,JSON_VALUE(data, 'prem_yoy') as prem_mom " +
                " ,JSON_VALUE(data, 'prem_yoy_rate') as prem_mom_rate " +
                " ,round(cast(JSON_VALUE(data2, 'val') as double) / 10000, 2) as val " +
                " ,JSON_VALUE(data2, 'val_yoy') as val_mom " +
                " ,JSON_VALUE(data2, 'val_yoy_rate') as val_mom_rate " +
                " from searchTable a " +
                " LEFT OUTER JOIN  " +
                " lateral table(es_search_object('tpwd_dx_policy_prem_down','grouping_code.keyword', a.grouping_code" +
                " ,'bus_model_code.keyword', a.bus_model_code" +
                " ,'stat_range.keyword', a.stat_range)) as Prem(data) on true " +
                " LEFT OUTER JOIN   " +
                "  lateral table(es_search_object('tpwd_dx_policy_value_down','grouping_code.keyword', JSON_VALUE(data, 'grouping_code') " +
                "  ,'bus_model_code.keyword', JSON_VALUE(data, 'bus_model_code') " +
                "  ,'stat_range.keyword', JSON_VALUE(data, 'stat_range'))) as Val(data2) on true ";

        logger.info(sql);
        // 执行聚合统计查询转换
        Table eggResult = tableEnv.sqlQuery(sql);

        eggResult.printSchema();

        // table 转流输出,聚合统计是动态表，所以使用Changelog的方式才能输出
        tableEnv.toChangelogStream(eggResult).print("egg");
        env.execute();
    }


    public static class SearchTable {
        private String grouping_code;
        private String bus_model_code;
        private String stat_range;

        public SearchTable() {

        }

        public SearchTable(String grouping_code, String bus_model_code, String stat_range){
            this.grouping_code = grouping_code;
            this.bus_model_code = bus_model_code;
            this.stat_range = stat_range;
        }

        public String getGrouping_code() {
            return grouping_code;
        }

        public void setGrouping_code(String grouping_code) {
            this.grouping_code = grouping_code;
        }

        public String getBus_model_code() {
            return bus_model_code;
        }

        public void setBus_model_code(String bus_model_code) {
            this.bus_model_code = bus_model_code;
        }

        public String getStat_range() {
            return stat_range;
        }

        public void setStat_range(String stat_range) {
            this.stat_range = stat_range;
        }
    }

}
