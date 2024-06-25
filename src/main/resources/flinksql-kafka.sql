-- 定义kafka数据源
CREATE TABLE kafka_study_table (
    log STRING,
    `data_store_time` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'study-info',
    'properties.bootstrap.servers' = 'cqdev01:9092,cqdev02:9092,cqdev03:9092',
    'properties.group.id' = 'flink-paimon-groupId',
    'scan.startup.mode' = 'latest-offset', --group-offsets:消费组偏移量
    'format' = 'raw' --读取全量字段为单一字段
);


CREATE TABLE ods_dp_study_info (
    dt DATE,
    data VARCHAR,
    data_store_time STRING
) WITH (
  'connector' = 'print'
);

-- 执行写入SQL语法
INSERT INTO ods_dp_study_info
    SELECT
         CURRENT_DATE AS dt
         ,log AS data
         ,DATE_FORMAT(data_store_time,'yyyy-MM-dd HH:mm:ss') AS data_store_time
    FROM kafka_study_table;
