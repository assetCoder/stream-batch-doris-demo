package com.example.streaming;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 作业配置 + Flink SQL 生成。
 *
 * <p>把“配置”和“SQL 拼接”单独抽出来，好处是：
 * 1. main 方法只负责编排，逻辑一目了然；
 * 2. 不启动 Flink 集群也能对 SQL 做单元测试；
 * 3. 所有环境变量只在这里解析，部署时不用改代码。
 *
 * <p>注意：SQL 里的字符串字面量必须转义单引号，否则密码/前缀里带引号会把 SQL 拼坏。
 */
public record JobConfig(
        String kafkaBootstrap,
        String kafkaTopic,
        String kafkaGroupId,
        String fenodes,
        String database,
        String table,
        String username,
        String password,
        String labelPrefix,
        long checkpointMs,
        int parallelism
) {

    /** 库名/表名只允许常规标识符，避免把用户输入直接拼进 SQL 标识符位置。 */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public JobConfig {
        Objects.requireNonNull(kafkaBootstrap, "kafkaBootstrap");
        Objects.requireNonNull(kafkaTopic, "kafkaTopic");
        Objects.requireNonNull(kafkaGroupId, "kafkaGroupId");
        Objects.requireNonNull(fenodes, "fenodes");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");
        Objects.requireNonNull(labelPrefix, "labelPrefix");
        if (!IDENTIFIER.matcher(database).matches()) {
            throw new IllegalArgumentException("DORIS_DATABASE 不是合法标识符: " + database);
        }
        if (!IDENTIFIER.matcher(table).matches()) {
            throw new IllegalArgumentException("DORIS_TABLE 不是合法标识符: " + table);
        }
        if (checkpointMs <= 0) {
            throw new IllegalArgumentException("CHECKPOINT_MS 必须大于 0");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("PARALLELISM 必须大于 0");
        }
    }

    /** 从环境变量读取配置；默认值面向本地 Docker Compose。 */
    public static JobConfig fromEnv() {
        return from(System.getenv());
    }

    /** 显式传入 Map，方便单测，不依赖真实环境变量。 */
    public static JobConfig from(Map<String, String> env) {
        return new JobConfig(
                value(env, "KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
                value(env, "KAFKA_TOPIC", "order-events"),
                value(env, "KAFKA_GROUP_ID", "flink-order-dwd"),
                value(env, "DORIS_FENODES", "127.0.0.1:8030"),
                value(env, "DORIS_DATABASE", "demo"),
                value(env, "DORIS_TABLE", "order_events"),
                value(env, "DORIS_USERNAME", "root"),
                value(env, "DORIS_PASSWORD", ""),
                value(env, "DORIS_LABEL_PREFIX", "flink-orders-"),
                Long.parseLong(value(env, "CHECKPOINT_MS", "10000")),
                Integer.parseInt(value(env, "PARALLELISM", "1"))
        );
    }

    private static String value(Map<String, String> env, String key, String defaultValue) {
        String v = env.get(key);
        return (v == null || v.isBlank()) ? defaultValue : v.trim();
    }

    /** SQL 单引号字面量转义：O'Reilly -> O''Reilly。 */
    private static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Kafka 源表。event_time 先按 STRING 读入，再在 SQL 里显式转 TIMESTAMP，
     * 这样源端格式错误时可以在 WHERE 里过滤，而不是让整个作业启动失败。
     */
    public String kafkaSourceDdl() {
        return """
                CREATE TABLE kafka_orders (
                    event_id    BIGINT,
                    user_id     BIGINT,
                    product_id  INT,
                    category    STRING,
                    amount      DECIMAL(12, 2),
                    quantity    INT,
                    channel     STRING,
                    event_time  STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = %s,
                    'properties.bootstrap.servers' = %s,
                    'properties.group.id' = %s,
                    'scan.startup.mode' = 'earliest-offset',
                    'format' = 'json',
                    'json.ignore-parse-errors' = 'true'
                )
                """.formatted(
                sqlString(kafkaTopic),
                sqlString(kafkaBootstrap),
                sqlString(kafkaGroupId)
        );
    }

    /**
     * Doris 目标表。列名、类型、顺序必须和 Doris 里的 order_events 完全一致；
     * 这里不写 PARTITION，因为示例表本身没有分区。
     */
    public String dorisSinkDdl() {
        return """
                CREATE TABLE doris_orders (
                    event_id    BIGINT,
                    user_id     BIGINT,
                    product_id  INT,
                    category    STRING,
                    amount      DECIMAL(12, 2),
                    quantity    INT,
                    channel     STRING,
                    event_time  TIMESTAMP(3),
                    dt          DATE
                ) WITH (
                    'connector' = 'doris',
                    'fenodes' = %s,
                    'table.identifier' = %s,
                    'username' = %s,
                    'password' = %s,
                    'sink.label-prefix' = %s,
                    'sink.enable-2pc' = 'true',
                    'sink.buffer-flush.max-rows' = '1000',
                    'sink.buffer-flush.interval' = '5s',
                    'sink.max-retries' = '3'
                )
                """.formatted(
                sqlString(fenodes),
                sqlString(database + "." + table),
                sqlString(username),
                sqlString(password),
                sqlString(labelPrefix)
        );
    }

    /**
     * 把 JSON 里的 event_time 字符串转成事件时间，并派生 dt。
     * 非法时间/缺失主键在这里过滤掉，避免坏数据反复触发 Doris 导入失败。
     */
    public String createViewSql() {
        return """
                CREATE TEMPORARY VIEW parsed_orders AS
                SELECT
                    event_id,
                    user_id,
                    product_id,
                    category,
                    amount,
                    quantity,
                    channel,
                    TO_TIMESTAMP(event_time) AS event_ts
                FROM kafka_orders
                """;
    }

    public String insertSql() {
        return """
                INSERT INTO doris_orders
                SELECT
                    event_id,
                    user_id,
                    product_id,
                    category,
                    amount,
                    quantity,
                    channel,
                    event_ts,
                    CAST(event_ts AS DATE)
                FROM parsed_orders
                WHERE event_ts IS NOT NULL
                  AND event_id IS NOT NULL
                  AND user_id IS NOT NULL
                """;
    }
}
