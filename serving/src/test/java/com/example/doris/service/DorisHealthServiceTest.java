package com.example.doris.service;

import com.example.doris.config.DorisProperties;
import com.example.doris.domain.HealthStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * 健康检查服务测试：不连真实 Doris，用 Mock 控制成功/失败。
 */
@ExtendWith(MockitoExtension.class)
class DorisHealthServiceTest {

    @Mock
    private JdbcTemplate jdbc;

    @Test
    void shouldReportUpWhenQueriesSucceed() {
        // 模拟 Doris 版本和表列表
        when(jdbc.queryForObject("SELECT version()", String.class)).thenReturn("4.1.5");
        when(jdbc.queryForList("SHOW TABLES", String.class)).thenReturn(List.of("order_events"));

        DorisProperties props = new DorisProperties();
        props.setDatabase("demo");

        HealthStatus status = new DorisHealthService(jdbc, props).check();

        assertThat(status.up()).isTrue();
        assertThat(status.version()).isEqualTo("4.1.5");
        assertThat(status.tables()).isEqualTo(1);
        assertThat(status.error()).isNull();
    }

    @Test
    void shouldReportDownWhenConnectionFails() {
        // 模拟连接失败：JdbcTemplate 会抛 DataAccessException
        when(jdbc.queryForObject("SELECT version()", String.class))
                .thenThrow(new DataAccessResourceFailureException("Connection refused"));

        DorisProperties props = new DorisProperties();
        props.setDatabase("demo");

        HealthStatus status = new DorisHealthService(jdbc, props).check();

        assertThat(status.up()).isFalse();
        assertThat(status.error()).contains("Connection refused");
        // 失败时仍然返回耗时，便于判断是快速失败还是超时
        assertThat(status.latencyMs()).isGreaterThanOrEqualTo(0);
    }
}
