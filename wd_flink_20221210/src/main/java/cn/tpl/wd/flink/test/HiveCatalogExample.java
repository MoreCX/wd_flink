package cn.tpl.wd.flink.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ChenXin
 * @create 2022-12-10-19:54
 */
public class HiveCatalogExample {
    private static final Logger logger = LoggerFactory.getLogger(HiveCatalogExample.class);

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "hive_test";
        String defaultDatabase = "flink_hive_database";
        String hiveConfDir     = "/etc/hive/conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("hive_test", hive);

        tableEnv.useCatalog("hive_test");

        tableEnv.useDatabase("flink_hive_database");

        String dropSql = "drop table if exists flink_es";
        tableEnv.executeSql(dropSql);

        String createSql = "CREATE TABLE  flink_es (" +
                "id INT NOT NULL primary key ," +
                "name  STRING ," +
                "dept_id int " +
                ")  WITH (" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://10.28.133.73:9208'," +
                "'index' = 'flink_es'," +
                "'username' = 'tpwd'," +
                "'password' = 'CT7Md1xc'" +
                ")";
        tableEnv.executeSql(createSql);

        String querySql = "select * from flink_hive_table_test_out_1";
        tableEnv.executeSql(querySql).print();

    }

}
