package com.example.doris.config;

// Spring 的配置类注解：告诉容器“这个类里有 @Bean 方法”
import org.springframework.context.annotation.Configuration;
// @Bean：把方法返回值注册成 Spring 容器里的对象
import org.springframework.context.annotation.Bean;
// HikariCP 的配置对象和连接池实现（spring-boot-starter-jdbc 已经自带）
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * 数据源配置。
 *
 * <p>Spring Boot 本来可以只用 application.yml 里的 spring.datasource.* 自动装配 DataSource，
 * 但这里手动创建，是为了让“连不上 Doris 也不阻止应用启动”这个教学点更直观，
 * 同时也把连接池参数和 DorisProperties 放在一起，避免两处配置不一致。</p>
 */
@Configuration
public class DorisDataSourceConfig {

    /**
     * 创建 HikariCP 连接池。
     *
     * @param props 已经绑定好的 Doris 配置
     * @return 由 Spring 管理的 DataSource；JdbcTemplate 会自动使用它
     */
    @Bean
    public DataSource dataSource(DorisProperties props) {
        // 创建 Hikari 配置对象，下面逐项设置
        HikariConfig config = new HikariConfig();

        // 连接池名字，日志和 JMX 里能看到，方便排查问题
        config.setPoolName("doris-pool");

        // JDBC URL：jdbc:mysql://fe:9030/demo?...
        config.setJdbcUrl(props.jdbcUrl());

        // 用户名和密码：Doris 的账号体系，生产环境请用只读/只写的最小权限账号
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());

        // 最大连接数：Doris 的查询并发不宜过高，教学示例给 10 个连接
        config.setMaximumPoolSize(props.getMaxPoolSize());

        // 最小空闲连接：保持 2 个热连接，减少首次查询的建连延迟
        config.setMinimumIdle(2);

        // 获取连接超时：Doris 挂掉时 3 秒内失败，而不是让 HTTP 请求一直等
        config.setConnectionTimeout(props.getConnectionTimeoutMs());

        // 连接有效性检测超时：借出连接前用 SELECT 1 验证，最多等 2 秒
        config.setValidationTimeout(2000);

        // 关键：-1 表示“启动时如果连不上数据库，也不要抛异常退出”
        // 这样即使 Doris 还没启动，Spring Boot 也能起来，/api/health 会如实报告 DOWN
        config.setInitializationFailTimeout(-1);

        // 连接测试语句：Doris 兼容 MySQL 协议，SELECT 1 是最轻量的探活
        config.setConnectionTestQuery("SELECT 1");

        // 用上面的配置创建连接池；Spring 容器关闭时会自动调用 close()
        return new HikariDataSource(config);
    }
}
