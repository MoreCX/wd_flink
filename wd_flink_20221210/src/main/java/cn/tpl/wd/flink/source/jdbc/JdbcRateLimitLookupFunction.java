package cn.tpl.wd.flink.source.jdbc;

import cn.tpl.wd.flink.metric.FlinkQpsMetricGauge;
import cn.tpl.wd.flink.util.FlinkLoggerUtil;
import cn.tpl.wd.flink.util.FlinkRateLimiter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;
/**
 * @author ChenXin
 * @create 2022-12-10-19:52
 */
public class JdbcRateLimitLookupFunction extends TableFunction<RowData> {
    private static final Logger LOG = FlinkLoggerUtil.getLogger();
    private static final long serialVersionUID = 1L;

    private final String query;
    private final JdbcConnectionProvider connectionProvider;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cacheMissingKey;
    private final long queryPerSecond;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private transient FieldNamedPreparedStatement statement;
    private transient Cache<RowData, List<RowData>> cache;
    private transient FlinkRateLimiter rateLimiter;
    private transient Counter lookUpCounter;
    private transient Counter lookUpRetryCounter;
    private transient Meter lookUpMeter;
    private transient FlinkQpsMetricGauge lookUpGauge;

    public JdbcRateLimitLookupFunction(
            JdbcConnectorOptions options,
            JdbcRateLimitLookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.connectionProvider = new SimpleJdbcConnectionProvider(options);
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.cacheMissingKey = lookupOptions.getCacheMissingKey();
        this.queryPerSecond = lookupOptions.getQueryPerSecond();
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        String dbURL = options.getDbURL();
        this.jdbcDialect = JdbcDialectLoader.load(dbURL);
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter =
                jdbcDialect.getRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            establishConnectionAndStatement();
            this.cache =
                    cacheMaxSize == -1 || cacheExpireMs == -1
                            ? null
                            : CacheBuilder.newBuilder()
                            .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                            .maximumSize(cacheMaxSize)
                            .build();
            this.rateLimiter = new FlinkRateLimiter(queryPerSecond);
            this.lookUpCounter = context.getMetricGroup().counter("jdbc-look-up-join-query-counter");
            this.lookUpRetryCounter = context.getMetricGroup().counter("jdbc-look-up-retry-join-query-counter");
            this.lookUpMeter = context.getMetricGroup()
                    .meter("jdbc-look-up-join-query-meter",
                            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            this.lookUpGauge = context.getMetricGroup().gauge("jdbc-look-up-join-query-qps", new FlinkQpsMetricGauge());

        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                rateLimiter.acquire();
                lookUpCounter.inc();
                if (retry != 0) {
                    this.lookUpRetryCounter.inc();
                }
                lookUpMeter.markEvent(1L);
                lookUpGauge.markEvent(1L);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (cache == null) {
                        while (resultSet.next()) {
                            collect(jdbcRowConverter.toInternal(resultSet));
                        }
                    } else {
                        ArrayList<RowData> rows = new ArrayList<>();
                        while (resultSet.next()) {
                            RowData row = jdbcRowConverter.toInternal(resultSet);
                            rows.add(row);
                            collect(row);
                        }
                        rows.trimToSize();
                        if (!rows.isEmpty() || cacheMissingKey) {
                            cache.put(keyRow, rows);
                        }
                    }
                }
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed",
                            exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection dbConn = connectionProvider.getOrEstablishConnection();
        statement = FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        connectionProvider.closeConnection();
    }

    @VisibleForTesting
    public Connection getDbConnection() {
        return connectionProvider.getConnection();
    }

    @VisibleForTesting
    public Cache<RowData, List<RowData>> getCache() {
        return cache;
    }

}
