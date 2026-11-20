-- Doris 分层示例：ODS/DWD 明细表 + ADS 指标表
-- 执行：mysql -h 127.0.0.1 -P 9030 -u root < infra/doris/init.sql

CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- Flink 实时链路写入的明细表。
-- DUPLICATE KEY：只追加，不去重；重复事件由上游 event_id 保证。
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
ENGINE=OLAP
DUPLICATE KEY(event_id, user_id)
COMMENT "订单事件明细表（Flink 写入）"
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Spark 离线链路写入的 ADS 表。
-- UNIQUE KEY(dt, channel)：同一天同一渠道重跑时覆盖，不会重复累加。
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
