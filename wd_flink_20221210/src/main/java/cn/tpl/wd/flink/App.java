package cn.tpl.wd.flink;

import cn.tpl.wd.flink.util.FlinkSqlExecutor;
import cn.tpl.wd.flink.util.FlinkSqlJobConfig;

/**
 * @author ChenXin
 * @create 2022-12-10-19:57
 *  * args example:
 *  * --sql sql1;sql2;sql3 --parallel 8 --job_name test_flink_sql_job
 */
public class App {
    public static void main(String[] args) {
        FlinkSqlJobConfig sqlJobConfig = FlinkSqlJobConfig.from(args);
        FlinkSqlExecutor flinkSqlExecutor = new FlinkSqlExecutor(sqlJobConfig);
        flinkSqlExecutor.executeSql();
        System.out.println("test git revert");
    }



}
