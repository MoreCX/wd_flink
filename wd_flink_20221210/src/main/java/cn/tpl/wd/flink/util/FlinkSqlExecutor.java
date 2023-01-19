package cn.tpl.wd.flink.util;

import cn.tpl.wd.flink.udf.DsgKafkaParseFunction;
import cn.tpl.wd.flink.udf.EsQueryFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.List;

/**
 * @author ChenXin
 * @create 2022-12-10-19:56
 */
public class FlinkSqlExecutor {
    private static Logger LOG = FlinkLoggerUtil.getLogger();

    private TableEnvironment tableEnv;
    private FlinkSqlJobConfig flinkSqlJobConfig;

    public FlinkSqlExecutor(FlinkSqlJobConfig sqlJobConfig) {
        this.flinkSqlJobConfig = sqlJobConfig;
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        settings.getConfiguration().setInteger("parallelism.default", flinkSqlJobConfig.getParallel());
        settings.getConfiguration().setString("pipeline.name", flinkSqlJobConfig.getJobName());
        tableEnv = TableEnvironment.create(settings);
        if(flinkSqlJobConfig.getUseHive()){
            initHive();
        }
        //通过client先注册到hive catalog上， 不使用hive catalog时 ，需注册
        if(!flinkSqlJobConfig.getUseHive()) {
            registerFunction();
        }
    }

    private void initHive(){
        HiveCatalog hive = new HiveCatalog(
                flinkSqlJobConfig.getCatalogName(),
                flinkSqlJobConfig.getCatalogDatabase(),
                flinkSqlJobConfig.getHiveConfDir()
        );
        tableEnv.registerCatalog(flinkSqlJobConfig.getCatalogName(), hive);
        tableEnv.useCatalog(flinkSqlJobConfig.getCatalogName());
        tableEnv.useDatabase(flinkSqlJobConfig.getCatalogDatabase());
    }


    private void registerFunction() {
        tableEnv.createTemporaryFunction("dsg_kafka_parse", DsgKafkaParseFunction.class);
        tableEnv.createTemporaryFunction("es_search_object", EsQueryFunction.class);
    }

    public void executeSql() {
        List<String> sqlList = flinkSqlJobConfig.getSqlList();
        LOG.info("receive sql:" + sqlList);
        Preconditions.checkArgument(sqlList.size() > 0, "sql can not be empty");
        for (String sql : sqlList) {
            LOG.info("execute sql:" + sql);
            Preconditions.checkArgument(sql.trim().length() > 0, "sql can not be empty");
            this.tableEnv.executeSql(sql);
        }
    }
}
