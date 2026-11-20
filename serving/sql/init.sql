-- ============================================================
-- 手动初始化脚本（可选）
-- 应用默认会通过 DorisSchemaInitializer 自动执行同样的 DDL；
-- 生产环境建议由 DBA 执行本文件，并把 demo.doris.init-schema 设为 false。
--
-- 执行方式：
--   mysql -h 127.0.0.1 -P 9030 -u root < sql/init.sql
-- ============================================================

-- 1. 建库
CREATE DATABASE IF NOT EXISTS demo;

-- 2. 切库
USE demo;

-- 3. 建表
--    DUPLICATE KEY：明细模型，只追加、不去重，适合订单事件日志
--    DISTRIBUTED BY HASH(user_id)：按 user_id 分桶，便于按用户聚合
--    replication_num=1：单 BE 本地演示；生产多 BE 建议设为 3
CREATE TABLE IF NOT EXISTS order_events (
    event_id    BIGINT        NOT NULL COMMENT "事件 ID",
    user_id     BIGINT        NOT NULL COMMENT "用户 ID",
    product_id  INT           NOT NULL COMMENT "商品 ID",
    category    VARCHAR(32)   NOT NULL COMMENT "商品品类",
    amount      DECIMAL(12,2) NOT NULL COMMENT "订单金额",
    quantity    INT           NOT NULL COMMENT "购买数量",
    channel     VARCHAR(16)   NOT NULL COMMENT "来源渠道",
    event_time  DATETIME      NOT NULL COMMENT "事件发生时间",
    dt          DATE          NOT NULL COMMENT "日期，便于按天聚合"
)
ENGINE = OLAP
DUPLICATE KEY(event_id, user_id)
COMMENT "订单事件明细表（教学示例，未分区）"
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);


-- ADS：Spark 离线作业写入，Spring Boot 查询
CREATE TABLE IF NOT EXISTS ads_daily_channel (
    dt               DATE          NOT NULL COMMENT "日期",
    channel          VARCHAR(16)   NOT NULL COMMENT "渠道",
    gmv              DECIMAL(18,2) NOT NULL COMMENT "GMV",
    orders           BIGINT        NOT NULL COMMENT "订单数",
    buyers           BIGINT        NOT NULL COMMENT "买家数",
    avg_buyer_value  DECIMAL(18,2) NOT NULL COMMENT "客单价"
)
ENGINE=OLAP
UNIQUE KEY(dt, channel)
COMMENT "日/渠道 ADS 指标（Spark 写入）"
DISTRIBUTED BY HASH(channel) BUCKETS 4
PROPERTIES ("replication_num" = "1");
