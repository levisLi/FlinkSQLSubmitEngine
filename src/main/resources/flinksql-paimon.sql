SET parallelism.default=1;
set execution.checkpointing.interval=10s;
set state.checkpoints.dir=file:///tmp/flink/checkpoints/flink-kafka-paimon;

-- 定义kafka数据源
CREATE TABLE kafka_study_table (
    log STRING,
    `data_store_time` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'study-info',
    'properties.bootstrap.servers' = 'cqdev01:9092,cqdev02:9092,cqdev03:9092',
    'properties.group.id' = 'flink-paimon-groupId',
    'format' = 'raw', --读取全量字段为单一字段
    'scan.startup.mode' = 'latest-offset'
);

-- 创建paimon的catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://ll03:9083',
    'warehouse' = 'hdfs://namespace-HA/user/hive/warehouse'
);

-- 不能USE CATALOG 否则后续kakfa表无法读取
-- USE CATALOG paimon_catalog;

-- 定义sink 数据源
CREATE TABLE IF NOT EXISTS paimon_catalog.paimon_ods.ods_dp_study_info (
    dt String,
    data String,
    data_store_time String
) PARTITIONED BY (dt)
WITH (
    'connector' = 'paimon',
    'file.format' = 'orc'
);

-- 执行写入SQL语法
INSERT INTO paimon_catalog.paimon_ods.ods_dp_study_info
    SELECT
         DATE_FORMAT(data_store_time,'yyyy-MM-dd') AS dt
         ,log AS data
         ,DATE_FORMAT(data_store_time,'yyyy-MM-dd HH:mm:ss') AS data_store_time
    FROM kafka_study_table;
