-- 定义kafka数据源
CREATE TABLE kafka_study_table (
    ris_study_id String,
    organ_code String,
    dev_name String,
    proctime as proctime()
) WITH (
    'connector' = 'kafka',
    'topic' = 'study-info',
    'properties.bootstrap.servers' = 'cqdev01:9092,cqdev02:9092,cqdev03:9092',
    'properties.group.id' = 'flink-sql-submit-groupId',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset', --group-offsets:消费组偏移量
    'json.ignore-parse-errors' = 'true'
);

-- 定义sink 数据源
CREATE TABLE print_table (
    organ_code String,
    cnt bigint,
    dt DATE,
    wEnd TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- 定义中间数据转化视图
CREATE VIEW study_cnt_10s_view AS
SELECT
    organ_code,
    COUNT(1) AS cnt,
    cast(DATE_FORMAT(TUMBLE_START(proctime, INTERVAL '10' SECOND),'yyyy-MM-dd') as DATE) as dt,
    TUMBLE_END(proctime, INTERVAL '10' SECOND) as wEnd,
    MAX(proctime) AS proctime
FROM kafka_study_table
GROUP BY organ_code, TUMBLE(proctime, INTERVAL '10' SECOND);

-- 执行写入SQL语法
INSERT INTO print_table SELECT organ_code,cnt,dt,wEnd  FROM study_cnt_10s_view;
