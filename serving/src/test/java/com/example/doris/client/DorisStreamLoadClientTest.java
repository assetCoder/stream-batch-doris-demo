package com.example.doris.client;

import com.example.doris.config.DorisProperties;
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.StreamLoadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 用 JDK 自带的 HttpServer 模拟 Doris FE，验证 Stream Load 的请求格式。
 *
 * <p>不需要真的启动 Doris 就能测：HTTP 方法、路径、认证头、JSON 字段、label 都能被断言。</p>
 */
class DorisStreamLoadClientTest {

    // 每个测试启动的临时 HTTP 服务
    private HttpServer server;

    @AfterEach
    void tearDown() {
        // 测试结束后关闭，避免端口泄漏
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldSendPutWithExpectedHeadersAndJson() throws Exception {
        // 用来捕获服务端收到的内容
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> auth = new AtomicReference<>();
        AtomicReference<String> label = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();

        // 启动一个随机端口的 HTTP 服务，模拟 FE 的 /_stream_load
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/demo/order_events/_stream_load", exchange -> {
            // 记录请求信息
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().getPath());
            auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            label.set(exchange.getRequestHeaders().getFirst("label"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

            // 返回 Doris 风格的 JSON
            byte[] response = """
                    {"Status":"Success","NumberLoadedRows":2,"NumberFilteredRows":0,"Message":"OK"}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        server.start();

        // 配置指向刚启动的临时服务
        DorisProperties props = new DorisProperties();
        props.setFeHost("127.0.0.1");
        props.setHttpPort(server.getAddress().getPort());
        props.setDatabase("demo");
        props.setUsername("root");
        props.setPassword("secret");
        props.setStreamLoadEnabled(true);

        // 准备两条数据
        List<OrderEvent> events = List.of(
                new OrderEvent(1L, 101L, 9001, "数码", new BigDecimal("2999.00"), 1, "douyin",
                        LocalDateTime.of(2026, 11, 20, 10, 0)),
                new OrderEvent(2L, 102L, 9002, "家电", new BigDecimal("899.50"), 1, "wechat",
                        LocalDateTime.of(2026, 11, 20, 11, 30))
        );

        // 调用客户端
        DorisStreamLoadClient client = new DorisStreamLoadClient(props, new ObjectMapper());
        StreamLoadResult result = client.load(events);

        // 断言 Doris 返回被正确解析
        assertThat(result.status()).isEqualTo("Success");
        assertThat(result.loadedRows()).isEqualTo(2);

        // 断言 HTTP 请求符合 Stream Load 规范
        assertThat(method.get()).isEqualTo("PUT");
        assertThat(path.get()).isEqualTo("/api/demo/order_events/_stream_load");
        assertThat(auth.get()).isEqualTo("Basic " +
                Base64.getEncoder().encodeToString("root:secret".getBytes(StandardCharsets.UTF_8)));
        assertThat(label.get()).startsWith("demo_");

        // 断言 JSON 字段名和时间格式
        assertThat(body.get())
                .contains("\"event_id\":1")
                .contains("\"event_time\":\"2026-11-20 10:00:00\"")
                .contains("\"dt\":\"2026-11-20\"");
    }

    @Test
    void shouldThrowWhenDorisReturnsFail() throws Exception {
        // 模拟 Doris 返回失败
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/demo/order_events/_stream_load", exchange -> {
            byte[] response = """
                    {"Status":"Fail","Message":"too many filtered rows"}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        server.start();

        DorisProperties props = new DorisProperties();
        props.setFeHost("127.0.0.1");
        props.setHttpPort(server.getAddress().getPort());
        props.setDatabase("demo");
        props.setUsername("root");
        props.setPassword("");

        DorisStreamLoadClient client = new DorisStreamLoadClient(props, new ObjectMapper());
        List<OrderEvent> events = List.of(
                new OrderEvent(3L, 103L, 9003, "图书", new BigDecimal("59.90"), 1, "organic",
                        LocalDateTime.of(2026, 11, 20, 12, 0))
        );

        // 失败状态必须抛 DorisException，而不是静默成功
        assertThatThrownBy(() -> client.load(events))
                .isInstanceOf(DorisException.class)
                .hasMessageContaining("too many filtered rows");
    }
}
