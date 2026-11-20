package com.example.doris.client;

// Jackson：Spring Boot 默认自带，用来把对象序列化成 JSON、把响应解析成树
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// 配置对象
import com.example.doris.config.DorisProperties;
// 领域对象
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.StreamLoadResult;

// Spring 组件注解
import org.springframework.stereotype.Component;

// HTTP 客户端（JDK 11+ 自带，不需要额外依赖）
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
// 字符集
import java.nio.charset.StandardCharsets;
// 时间
import java.time.Duration;
// Base64 编码
import java.util.Base64;
// 集合
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

// JSON 解析异常
import com.fasterxml.jackson.core.JsonProcessingException;
// 网络异常
import java.io.IOException;

/**
 * Doris Stream Load 客户端。
 *
 * <p>Stream Load 是 Doris 官方推荐的“大批量导入”方式：</p>
 * <ul>
 *   <li>走 FE 的 HTTP 端口（默认 8030），不是 MySQL 9030；</li>
 *   <li>一次 PUT 提交一批数据，同步返回导入结果；</li>
 *   <li>通过 label 做幂等：相同 label 重复提交，Doris 不会重复写入。</li>
 * </ul>
 *
 * <p>本类用 JDK 自带的 HttpClient，避免再引入 OkHttp/RestTemplate 依赖。</p>
 */
@Component
public class DorisStreamLoadClient {

    // Stream Load 的列顺序，必须与 jsonpaths 一一对应
    private static final String COLUMNS =
            "event_id,user_id,product_id,category,amount,quantity,channel,event_time,dt";

    // 告诉 Doris：从 JSON 的哪些字段取值；顺序与上面的 columns 对应
    private static final String JSON_PATHS =
            "[\"$.event_id\",\"$.user_id\",\"$.product_id\",\"$.category\","
            + "\"$.amount\",\"$.quantity\",\"$.channel\",\"$.event_time\",\"$.dt\"]";

    // Doris DATETIME 接受的格式：yyyy-MM-dd HH:mm:ss（用空格，不是 T）
    private static final java.time.format.DateTimeFormatter DATETIME_FORMAT =
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 配置：FE 地址、8030 端口、库名、账号密码、超时
    private final DorisProperties props;

    // Jackson 对象映射器：负责 Java 对象 <-> JSON
    private final ObjectMapper objectMapper;

    // JDK HttpClient：线程安全，整个应用复用一个即可
    private final HttpClient httpClient;

    /**
     * 构造器注入配置和 Jackson。
     *
     * @param props        Doris 配置
     * @param objectMapper Spring Boot 自动配置的 Jackson
     */
    public DorisStreamLoadClient(DorisProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;

        // 建连超时 3 秒；强制 HTTP/1.1，避免某些 FE 版本对 HTTP/2 支持不完整
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    /**
     * 把一批订单事件通过 Stream Load 写入 Doris。
     *
     * @param events 订单事件列表，不能为空
     * @return Doris 的导入结果
     */
    public StreamLoadResult load(List<OrderEvent> events) {
        // 配置里关闭了 Stream Load 时，直接给出明确错误，而不是发一个必然失败的请求
        if (!props.isStreamLoadEnabled()) {
            throw new DorisException("Stream Load 已被配置关闭：demo.doris.stream-load-enabled=false");
        }

        // 空列表没有导入意义，提前拦截
        if (events == null || events.isEmpty()) {
            throw new DorisException("Stream Load 的 events 不能为空");
        }

        // 每次导入生成唯一 label；保留 UUID 的连字符也合法，这里去掉让日志更短
        String label = "demo_" + System.currentTimeMillis() + "_"
                + UUID.randomUUID().toString().replace("-", "");

        // 把 OrderEvent 转成“蛇形命名”的 Map，字段名与 Doris 列名一致
        List<Map<String, Object>> rows = events.stream().map(event -> {
            // LinkedHashMap 保证 JSON 字段顺序与 COLUMNS 一致，方便排查
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("event_id", event.eventId());
            row.put("user_id", event.userId());
            row.put("product_id", event.productId());
            row.put("category", event.category());
            row.put("amount", event.amount());
            row.put("quantity", event.quantity());
            row.put("channel", event.channel());
            // Doris DATETIME 用空格分隔，不用 ISO 的 T
            row.put("event_time", event.eventTime().format(DATETIME_FORMAT));
            // DATE 用 yyyy-MM-dd
            row.put("dt", event.dt().toString());
            return row;
        }).toList();

        // 序列化成 JSON 数组；strip_outer_array=true 时 Doris 期望最外层是数组
        String body;
        try {
            body = objectMapper.writeValueAsString(rows);
        } catch (JsonProcessingException e) {
            // 正常不会发生；一旦发生说明字段里有无法序列化的类型
            throw new DorisException("序列化 Stream Load 数据失败：" + e.getMessage());
        }

        // 构造 HTTP 请求
        HttpRequest request = HttpRequest.newBuilder()
                // Stream Load 地址：http://fe:8030/api/{db}/{table}/_stream_load
                .uri(URI.create(props.streamLoadUrl()))
                // 单次导入超时；大批量时把 demo.doris.stream-load-timeout-seconds 调大
                .timeout(Duration.ofSeconds(props.getStreamLoadTimeoutSeconds()))
                // Basic 认证：用户名:密码 的 Base64
                .header("Authorization", basicAuth(props.getUsername(), props.getPassword()))
                // 数据格式为 JSON
                .header("format", "json")
                // 最外层是数组，需要告诉 Doris 拆开
                .header("strip_outer_array", "true")
                // 从 JSON 的哪些路径取值
                .header("jsonpaths", JSON_PATHS)
                // 取出的值按这个列顺序写入
                .header("columns", COLUMNS)
                // 幂等标签：相同 label 重复提交不会重复写入
                .header("label", label)
                // 说明：curl 示例常带 Expect: 100-continue；JDK HttpClient 会自行处理
                // 请求体发送，这里不显式设置，避免不同 FE/代理对 100-continue 支持不一致
                // 声明请求体类型
                .header("Content-Type", "application/json")
                // PUT 是 Stream Load 规定的 HTTP 方法
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        // 发送请求并拿到字符串响应
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // 网络层失败：FE 不可达、超时、连接被重置等
            throw new DorisException("调用 Doris Stream Load 失败：" + e.getMessage());
        } catch (InterruptedException e) {
            // 恢复中断标记，避免吞掉线程的中断信号
            Thread.currentThread().interrupt();
            throw new DorisException("调用 Doris Stream Load 被中断");
        }

        // 非 2xx 直接带上响应体抛出，便于定位是权限、库表还是格式问题
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new DorisException("Doris Stream Load 返回非 2xx",
                    response.statusCode(), truncate(response.body()));
        }

        // 解析 Doris 的 JSON 响应
        JsonNode json;
        try {
            json = objectMapper.readTree(response.body());
        } catch (JsonProcessingException e) {
            throw new DorisException("Doris 响应不是合法 JSON：" + truncate(response.body()),
                    response.statusCode(), response.body());
        }

        // Doris 用 Status 字段表示导入结果，成功值是 "Success"
        String status = json.path("Status").asText("Unknown");
        // NumberLoadedRows 可能不存在（失败时），asLong(0) 提供默认值
        long loadedRows = json.path("NumberLoadedRows").asLong(0);
        long filteredRows = json.path("NumberFilteredRows").asLong(0);
        String message = json.path("Message").asText("");

        // 只有 Success 才认为成功；Publish Timeout 表示事务已提交但版本未发布，
        // 生产环境应轮询 SHOW TRANSACTION 或重试查询，这里为教学简化。
        if (!"Success".equalsIgnoreCase(status)) {
            throw new DorisException("Doris Stream Load 失败：status=" + status + ", message=" + message,
                    response.statusCode(), truncate(response.body()));
        }

        // 返回结构化结果，Controller 直接序列化给调用方
        return new StreamLoadResult(label, status, loadedRows, filteredRows, message);
    }

    /**
     * 生成 Basic 认证头。
     *
     * @param username 用户名
     * @param password 密码
     * @return 形如 "Basic base64(user:pass)"
     */
    private String basicAuth(String username, String password) {
        // 用户名密码用冒号连接，再做 Base64
        String raw = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 截断过长的响应体，避免异常消息和日志被几 MB 的 HTML 撑爆。
     *
     * @param body 原始响应
     * @return 最多 500 个字符
     */
    private String truncate(String body) {
        // null 安全：网络异常时 body 可能为 null
        if (body == null) {
            return "";
        }
        // 超过 500 字符就截断并加省略号
        return body.length() <= 500 ? body : body.substring(0, 500) + "...";
    }
}
