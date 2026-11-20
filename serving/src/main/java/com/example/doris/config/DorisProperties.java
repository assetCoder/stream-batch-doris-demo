package com.example.doris.config;

// Spring Boot 的配置绑定注解：把 application.yml 中以 demo.doris 开头的配置映射到本类字段
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Doris 连接配置。
 *
 * <p>所有字段都能在 application.yml 里覆盖，也支持环境变量（见 application.yml 的 ${DORIS_*:默认值} 写法）。</p>
 *
 * <p>为什么不用 spring.datasource.*？<br>
 * 因为我们要自己控制 Hikari 的“启动时连不上也不退出”等参数，所以用这个类集中管理，
 * 再在 {@link DorisDataSourceConfig} 里手动创建 DataSource。这样配置只写一份，JDBC 和 Stream Load 共用。</p>
 */
@ConfigurationProperties(prefix = "demo.doris")
public class DorisProperties {

    /** FE 节点地址：可以是 IP、域名，或者前面挂的 VIP/负载均衡地址 */
    private String feHost = "127.0.0.1";

    /** FE 的 MySQL 协议端口，默认 9030；JDBC 查询、INSERT 都走这个端口 */
    private int queryPort = 9030;

    /** FE 的 HTTP 端口，默认 8030；Stream Load 批量导入走这个端口 */
    private int httpPort = 8030;

    /** 目标数据库名；应用启动时可以自动创建（见 DorisSchemaInitializer） */
    private String database = "demo";

    /** Doris 用户名；本地默认 root，生产环境请换成最小权限账号 */
    private String username = "root";

    /** Doris 密码；本地默认空字符串，生产环境请通过环境变量注入，不要硬编码 */
    private String password = "";

    /** 是否在启动时自动创建数据库和示例表；生产环境通常设为 false，由 DBA 执行 DDL */
    private boolean initSchema = true;

    /** HikariCP 最大连接数；Doris 适合少量长连接 + 大批量导入，不建议开太大 */
    private int maxPoolSize = 10;

    /** 获取连接的超时时间（毫秒）；Doris 不可达时尽快失败，避免接口一直挂起 */
    private long connectionTimeoutMs = 3000;

    /** 是否启用 Stream Load 接口；关闭后 /api/orders/stream-load 会直接报错 */
    private boolean streamLoadEnabled = true;

    /** Stream Load 的 HTTP 超时时间（秒）；大批量导入要适当调大 */
    private int streamLoadTimeoutSeconds = 60;

    /**
     * JDBC 公共参数。
     *
     * <ul>
     *   <li>useUnicode/characterEncoding：保证中文正常</li>
     *   <li>useSSL=false：本地演示关闭 SSL；生产环境应开启并配置证书</li>
     *   <li>allowPublicKeyRetrieval：MySQL 8 驱动首次连接时需要</li>
     *   <li>serverTimezone：避免 DATETIME 因时区偏移 8 小时</li>
     *   <li>connectTimeout/socketTimeout：连接和读超时，单位毫秒</li>
     *   <li>rewriteBatchedStatements：让批量 INSERT 合并成多值语句，减少网络往返</li>
     * </ul>
     */
    private static final String COMMON_JDBC_PARAMS =
            "useUnicode=true"
            + "&characterEncoding=utf8"
            + "&useSSL=false"
            + "&allowPublicKeyRetrieval=true"
            + "&serverTimezone=Asia/Shanghai"
            + "&connectTimeout=3000"
            + "&socketTimeout=30000"
            + "&rewriteBatchedStatements=true";

    /**
     * 带数据库名的 JDBC URL，供业务查询使用。
     *
     * @return 形如 jdbc:mysql://127.0.0.1:9030/demo?...
     */
    public String jdbcUrl() {
        // 拼接 FE 地址、查询端口、数据库名和公共参数
        return "jdbc:mysql://" + feHost + ":" + queryPort + "/" + database + "?" + COMMON_JDBC_PARAMS;
    }

    /**
     * 不带数据库名的 JDBC URL，只用于“创建数据库”这种初始化动作。
     *
     * <p>因为数据库还不存在时，如果 URL 里带了库名，连接会直接失败。</p>
     *
     * @return 形如 jdbc:mysql://127.0.0.1:9030/?...
     */
    public String serverJdbcUrl() {
        // 注意这里是 "/?"，斜杠后面直接跟问号，表示不指定 database
        return "jdbc:mysql://" + feHost + ":" + queryPort + "/?" + COMMON_JDBC_PARAMS;
    }

    /**
     * Stream Load 的完整 URL。
     *
     * <p>路径格式固定为：/api/{database}/{table}/_stream_load，必须走 FE 的 HTTP 端口（默认 8030），
     * 不是 MySQL 端口 9030。</p>
     *
     * @return 形如 http://127.0.0.1:8030/api/demo/order_events/_stream_load
     */
    public String streamLoadUrl() {
        // 拼接 HTTP 协议、FE 地址、HTTP 端口、库名、表名和固定后缀
        return "http://" + feHost + ":" + httpPort + "/api/" + database + "/order_events/_stream_load";
    }

    // ==================== 下面是标准 getter/setter，Spring Boot 绑定配置时使用 ====================

    public String getFeHost() {
        return feHost;
    }

    public void setFeHost(String feHost) {
        this.feHost = feHost;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public void setQueryPort(int queryPort) {
        this.queryPort = queryPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isInitSchema() {
        return initSchema;
    }

    public void setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(long connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public boolean isStreamLoadEnabled() {
        return streamLoadEnabled;
    }

    public void setStreamLoadEnabled(boolean streamLoadEnabled) {
        this.streamLoadEnabled = streamLoadEnabled;
    }

    public int getStreamLoadTimeoutSeconds() {
        return streamLoadTimeoutSeconds;
    }

    public void setStreamLoadTimeoutSeconds(int streamLoadTimeoutSeconds) {
        this.streamLoadTimeoutSeconds = streamLoadTimeoutSeconds;
    }
}
