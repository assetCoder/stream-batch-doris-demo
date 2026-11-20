package com.example.doris.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.example.doris.config.DorisProperties;
import com.example.doris.domain.HealthStatus;

import java.util.List;

/**
 * Doris 健康检查。
 *
 * <p>应用能启动不代表 Doris 可用，所以用几条最轻量的 SQL 做探活：</p>
 * <ul>
 *   <li>SELECT version()：验证连接和查询权限；</li>
 *   <li>SHOW TABLES：验证目标库存在，顺便统计表数量。</li>
 * </ul>
 */
@Service
public class DorisHealthService {

    // JdbcTemplate 由 Spring Boot 自动配置
    private final JdbcTemplate jdbc;

    // 配置里拿数据库名，用于展示
    private final DorisProperties props;

    /**
     * 构造器注入。
     *
     * @param jdbc  JDBC 模板
     * @param props Doris 配置
     */
    public DorisHealthService(JdbcTemplate jdbc, DorisProperties props) {
        this.jdbc = jdbc;
        this.props = props;
    }

    /**
     * 执行探活。
     *
     * @return 健康状态；失败时不抛异常，而是把原因放进 error 字段
     */
    public HealthStatus check() {
        // 记录开始时间，用来算耗时
        long start = System.nanoTime();

        try {
            // 查询 Doris 版本；能返回说明 TCP、认证、查询都正常
            String version = jdbc.queryForObject("SELECT version()", String.class);

            // SHOW TABLES 返回当前库的表名列表；取第一列映射成 String
            List<String> tables = jdbc.queryForList("SHOW TABLES", String.class);

            // 计算耗时（纳秒转毫秒）
            long latencyMs = (System.nanoTime() - start) / 1_000_000;

            // 组装成功结果；error 为 null
            return new HealthStatus(true, version, props.getDatabase(), tables.size(), latencyMs, null);

        } catch (RuntimeException e) {
            // 计算耗时，失败时也返回，方便判断是“立即失败”还是“超时”
            long latencyMs = (System.nanoTime() - start) / 1_000_000;

            // 取最内层异常消息，避免只看到 JdbcTemplate 的外层包装
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }

            // 返回 DOWN，不抛异常；Controller 统一返回 503
            return new HealthStatus(false, null, props.getDatabase(), 0, latencyMs, root.getMessage());
        }
    }
}
