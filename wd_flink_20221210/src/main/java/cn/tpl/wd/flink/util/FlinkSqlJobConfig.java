package cn.tpl.wd.flink.util;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ChenXin
 * @create 2022-12-10-19:56
 */
public class FlinkSqlJobConfig {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlJobConfig.class);

    @Option(name = "--use_hive", required = false, usage = "use hive catalog,true/false,default true")
    private Boolean useHive = true;

    @Option(name = "--hive_catalog", required = false, usage = "hive catalog name,default hive_test")
    private String catalogName = "hive_test";

    @Option(name = "--hive_db", required = false, usage = "hive db name,default flink_hive_database")
    private String catalogDatabase = "flink_hive_database";

    @Option(name = "--hive_conf", required = false, usage = "hive conf path,default /etc/hive/conf")
    private String hiveConfDir     = "/etc/hive/conf";

    @Option(name = "--sql_path", required = true, usage = "execute sql path")
    private String sqlPath;

    @Option(name = "--parallel", required = true, usage = "flink job parallel ")
    private Integer parallel;

    @Option(name = "--job_name", required = true, usage = "flink job name ")
    private String jobName;

    private List<String> sqlList;
    private Map<String, Object> envConfig = new HashMap<>();

    public static FlinkSqlJobConfig from(String[] args) {
        FlinkSqlJobConfig flinkSqlJobConfig = new FlinkSqlJobConfig();
        CmdLineParser parser = new CmdLineParser(flinkSqlJobConfig);
        try {
            parser.parseArgument(args);
            System.out.println("job_name:" + flinkSqlJobConfig.jobName);
            System.out.println("sql_path:" + flinkSqlJobConfig.sqlPath);
            System.out.println("parallel:" + flinkSqlJobConfig.parallel);
            File sqlFile = new File(flinkSqlJobConfig.sqlPath);
            String sqlContent = FileUtils.readFileToString(sqlFile, Charset.defaultCharset());
            flinkSqlJobConfig.sqlList = Arrays.asList(sqlContent.split(";"));
            flinkSqlJobConfig.sqlList.forEach(System.out::println);
        } catch (CmdLineException | IOException e) {
            System.err.println(e.getMessage());
            System.err.println("Sample [options...] arguments...");
            parser.printUsage(System.err);
            System.err.println();
            e.printStackTrace();
        }
        return flinkSqlJobConfig;
    }

    public String getSqlPath() {
        return sqlPath;
    }

    public Integer getParallel() {
        return parallel;
    }

    public String getJobName() {
        return jobName;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public Boolean getUseHive() {
        return useHive;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogDatabase() {
        return catalogDatabase;
    }

    public String getHiveConfDir() {
        return hiveConfDir;
    }

    public Map<String, Object> getEnvConfig() {
        return envConfig;
    }

}
