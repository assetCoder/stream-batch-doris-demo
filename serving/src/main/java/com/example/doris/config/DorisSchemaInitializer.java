package com.example.doris.config;

// Spring 的组件注解：启动时自动扫描并注册这个类
import org.springframework.stereotype.Component;
// ApplicationRunner：Spring 容器启动完成后执行 run()，适合做建库建表这种初始化
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ApplicationArguments;

// SLF4J 日志接口，Spring Boot 默认用 Logback 实现
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// JDBC 基础类：直接用 DriverManager 拿连接，不经过 Hikari 池
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 启动时自动建库建表（可选）。
 *
 * <p>生产环境一般由 DBA 执行 DDL，把 demo.doris.init-schema 设为 false 即可。
 * 本地学习时打开它，第一次启动就能得到可用的库表。</p>
 *
 * <p>注意：这里故意用 DriverManager 而不是 Hikari。因为 Hikari 的 URL 指向 demo 库，
 * 而建库时 demo 还不存在；用不带库名的 serverJdbcUrl 更直接。</p>
 */
@Component
public class DorisSchemaInitializer implements ApplicationRunner {

    // 日志对象，用来输出“建表成功/跳过/失败”
    private static final Logger log = LoggerFactory.getLogger(DorisSchemaInitializer.class);

    // 建表 DDL：与 sql/init.sql 中的内容保持一致
    private static final String CREATE_TABLE_SQL = """
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
            COMMENT "订单事件明细表（教学示例，未分区）"
            DISTRIBUTED BY HASH(user_id) BUCKETS 8
            PROPERTIES (
                "replication_num" = "1"
            );
            """;

    // ADS 表：Spark 离线作业写入，Spring Boot 只读；Unique Key 保证重跑同一天不翻倍
    private static final String CREATE_ADS_TABLE_SQL = """
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
            PROPERTIES (
                "replication_num" = "1"
            );
            """;

    // 配置对象由 Spring 注入
    private final DorisProperties props;

    /**
     * 构造器注入：Spring 会自动把 DorisProperties 传进来。
     *
     * @param props Doris 配置
     */
    public DorisSchemaInitializer(DorisProperties props) {
        this.props = props;
    }

    /**
     * 容器启动后执行：建库 -> 切库 -> 建表。
     *
     * @param args 启动参数，本示例不用
     */
    @Override
    public void run(ApplicationArguments args) {
        // 如果配置关闭了自动初始化，打印一行日志后直接返回
        if (!props.isInitSchema()) {
            log.info("跳过 Doris 建表：demo.doris.init-schema=false");
            return;
        }

        // 数据库名只允许字母、数字、下划线，避免把配置拼进 DDL 时产生注入风险
        if (!props.getDatabase().matches("[A-Za-z0-9_]+")) {
            log.warn("数据库名不合法，跳过自动建表：{}", props.getDatabase());
            return;
        }

        // try-with-resources：无论成功失败都会自动关闭 Connection 和 Statement
        try (Connection connection = DriverManager.getConnection(
                props.serverJdbcUrl(), props.getUsername(), props.getPassword());
             Statement statement = connection.createStatement()) {

            // 1) 建库：IF NOT EXISTS 保证重复启动不报错
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + props.getDatabase() + "`");

            // 2) 切到目标库；同一个 Statement 后续的 DDL 都会作用在这个库上
            statement.execute("USE `" + props.getDatabase() + "`");

            // 3) 明细表：DUPLICATE KEY 模型适合“只追加、不去重”的明细日志
            statement.execute(CREATE_TABLE_SQL);

            // 4) ADS 表：Unique Key，Spark 覆盖写入时按 (dt, channel) 做 upsert
            statement.execute(CREATE_ADS_TABLE_SQL);

            // 成功日志：把库名和表名打出来，方便确认连的是哪个集群
            log.info("Doris 初始化完成：database={}, tables=order_events,ads_daily_channel", props.getDatabase());

        } catch (SQLException e) {
            // 初始化失败不阻断启动：应用仍可提供 /api/health，便于排查网络/权限问题
            log.warn("Doris 初始化失败（应用会继续启动，请检查 FE 地址、端口和账号）：{}", e.getMessage());
        }
    }
}
