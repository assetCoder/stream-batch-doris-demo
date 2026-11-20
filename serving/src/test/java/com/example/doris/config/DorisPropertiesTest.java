package com.example.doris.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DorisProperties 的 URL 拼接测试。
 *
 * <p>这些 URL 一旦拼错，应用会在运行时才暴露问题，所以用单测固定住格式。</p>
 */
class DorisPropertiesTest {

    @Test
    void shouldBuildJdbcAndStreamLoadUrls() {
        // 准备一份配置
        DorisProperties props = new DorisProperties();
        props.setFeHost("doris-fe");
        props.setQueryPort(9030);
        props.setHttpPort(8030);
        props.setDatabase("demo");

        // 带库名的 JDBC URL
        assertThat(props.jdbcUrl())
                .startsWith("jdbc:mysql://doris-fe:9030/demo?")
                .contains("useUnicode=true")
                .contains("serverTimezone=Asia/Shanghai");

        // 不带库名的 JDBC URL：斜杠后直接是问号
        assertThat(props.serverJdbcUrl())
                .startsWith("jdbc:mysql://doris-fe:9030/?")
                .doesNotContain("/demo");

        // Stream Load 必须走 8030 的 HTTP 端口
        assertThat(props.streamLoadUrl())
                .isEqualTo("http://doris-fe:8030/api/demo/order_events/_stream_load");
    }
}
