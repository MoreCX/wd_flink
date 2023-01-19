CREATE TABLE KAFKA_SOURCE_TABLE
(

    `user_id` BIGINT,
    `dept_no` BIGINT,
    `item_id` BIGINT,
    `proc_time` AS PROCTIME(),
    `rowtime`   TIMESTAMP(3),
    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '5' MINUTES,
    PRIMARY KEY (`user_id`) NOT ENFORCED
)
    WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'upsert-kafka-test1',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'test_log',
        'value.json.timestamp-format.standard' = 'SQL',
        'key.format' = 'json',
        'key.json.ignore-parse-errors' = 'true',
        'value.format' = 'json',
        'value.json.fail-on-missing-field' = 'false');
CREATE TEMPORARY TABLE ORACLE_JDBC_TABLE
(
    ID BIGINT,
    NAME STRING,
    REGION STRING
)
WITH (
    'connector' = 'rate-limit-jdbc',
    'url' = 'jdbc:oracle:thin:@localhost:1521:XE',
    'table-name' = 'wangyiyi.dept_mapping1',
    'username' = 'wangyiyi',
    'password' = '1025',
    'lookup.rate-limit.query-per-second' = '10');
CREATE VIEW JOINED_TABLE AS
(
SELECT A.user_id, B.NAME, A.rowtime
FROM KAFKA_SOURCE_TABLE AS A
         LEFT JOIN ORACLE_JDBC_TABLE FOR SYSTEM_TIME AS OF A.proc_time AS B
                   ON A.dept_no=B.ID);
CREATE TABLE PRINT_TABLE
(
    USER_ID BIGINT,
    NAME STRING,
    ROWTIME TIMESTAMP(3)
)
    WITH ('connector' = 'print');
INSERT INTO PRINT_TABLE
SELECT *
FROM JOINED_TABLE

