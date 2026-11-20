# Spring Boot + Apache Doris 教学服务

> 一个可以直接跑、可以照着改的 Doris 入门服务：JDBC 查询 + Stream Load 批量导入，所有核心代码几乎逐行中文注释。

## 先回答：Doris 有 Spring Boot Starter 吗？

**没有官方 Starter。** 截至 2026-11，Maven Central 上 Apache Doris 只发布了这些 Java 相关构件：

| 构件 | 用途 | 能不能当 Spring Boot Starter |
| --- | --- | --- |
| `flink-doris-connector-*` | Flink 读写 Doris | 不能，Flink 专用 |
| `spark-doris-connector-*` | Spark 读写 Doris | 不能，Spark 专用 |
| `doris-kafka-connector` | Kafka Connect 写入 Doris | 不能 |
| `org.apache.doris:doris-sdk` | Thrift SDK（FE/BE 元数据、内部协议） | 不能，不是 Spring 封装 |
| `spring-shade-5` | Doris 自己 shade 的 Spring 依赖 | 不能，不是给业务应用用的 |

在 Maven Central 搜 `doris-spring` / `doris-spring-boot-starter` 是 0 个结果；GitHub 上也没有官方仓库。社区有一些个人封装的 starter，但基本不在 Central、维护频率低，生产上不建议把数据访问层交给这种“黑盒”。

### 那“最稳的 starter”是什么？

**Spring Boot 官方的 `spring-boot-starter-jdbc` + MySQL Connector/J。** 原因很简单：

- Doris FE 默认在 **9030** 端口暴露 **MySQL 协议**，官方文档明确推荐 Java 应用用 MySQL JDBC Connector 连接；
- JDBC 是标准协议，Spring 的 `JdbcTemplate`、HikariCP、事务/异常体系都能直接用；
- 批量导入再叠加 Doris 原生的 **Stream Load**（HTTP 8030），这才是 Doris 的正确用法，不需要额外 starter。

官方依据：

- [Doris MySQL 协议文档](https://doris.apache.org/docs/4.x/connection-integration/mysql-proto/)：FE 的 9030 端口兼容 MySQL 协议，Java 推荐 MySQL JDBC Connector；
- [Stream Load 文档](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)：`PUT http://fe:8030/api/{db}/{table}/_stream_load`；
- [Doris Releases](https://github.com/apache/doris/releases)：本示例对齐 4.1.5（2026-10）。

## 版本矩阵

| 组件 | 版本 | 选择理由 |
| --- | --- | --- |
| Spring Boot | **3.5.7** | 3.5.x 是当前最稳的 3.x 线，教程/生态最全；4.x 也能跑，代码不用改 |
| Java | **21** | 当前 LTS，Spring Boot 3.x 官方支持 |
| MySQL Connector/J | **8.4.0** | MySQL 的 LTS 驱动，兼容 Doris 的 MySQL 协议；9.x 也能用，但 8.4 更保守 |
| Apache Doris | **4.1.5** | 2026-10 的最新稳定版；3.x/2.x 的 SQL 也基本兼容 |

> 为什么不追 Spring Boot 4.2？这个服务的重点是 Doris 集成，不是 Spring 新特性；3.5.x 能让读者把注意力放在 SQL、连接池和导入链路上。升级 4.x 时，`JdbcTemplate`、Hikari、Jackson 的用法不变。

## 这个服务能做什么

```text
POST /api/orders                 单条写入（JDBC INSERT）
POST /api/orders/batch           批量写入（JDBC batch）
POST /api/orders/stream-load     批量导入（Doris Stream Load，HTTP 8030）
GET  /api/orders/{eventId}       按 ID 查询
GET  /api/orders?page=1&size=20  分页查询
GET  /api/analytics/summary      GMV / 订单数 / 买家数 / 客单价
GET  /api/analytics/top-products 商品 Top N
GET  /api/analytics/daily        按天趋势
GET  /api/health                 Doris 健康检查
```

## 目录结构

```text
doris-demo/
├── pom.xml
├── run.sh
├── sql/
│   ├── init.sql                  手动建库建表（可选）
│   └── seed.sql                  10 条演示数据
└── src/
    ├── main/java/com/example/doris/
    │   ├── DorisDemoApplication.java      启动类
    │   ├── config/
    │   │   ├── DorisProperties.java       连接配置 + URL 拼接（逐行注释）
    │   │   ├── DorisDataSourceConfig.java HikariCP 连接池（逐行注释）
    │   │   └── DorisSchemaInitializer.java 启动时建库建表
    │   ├── domain/                        record DTO
    │   ├── repository/
    │   │   └── DorisOrderRepository.java  所有 SQL（逐行注释）
    │   ├── client/
    │   │   ├── DorisStreamLoadClient.java Stream Load HTTP 客户端（逐行注释）
    │   │   └── DorisException.java
    │   ├── service/
    │   │   ├── OrderService.java          写入/分页边界
    │   │   ├── AnalyticsService.java      日期范围/聚合
    │   │   └── DorisHealthService.java    探活
    │   └── web/                           Controller + 全局异常
    ├── main/resources/application.yml
    └── test/java/...                      16 个不依赖 Doris 的单测
```

## 数据模型：为什么用 DUPLICATE KEY

```sql
CREATE TABLE order_events (
    event_id    BIGINT,
    user_id     BIGINT,
    product_id  INT,
    category    VARCHAR(32),
    amount      DECIMAL(12,2),
    quantity    INT,
    channel     VARCHAR(16),
    event_time  DATETIME,
    dt          DATE
)
ENGINE = OLAP
DUPLICATE KEY(event_id, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
```

- **DUPLICATE KEY**：明细模型，只追加、不去重。订单事件日志天然是“发生过就算数”，重复导入同一 `event_id` 会出现多行（生产上要用唯一键或导入幂等解决）。
- **DISTRIBUTED BY HASH(user_id)**：按用户分桶，`WHERE user_id = ?` 和按用户聚合能命中同一个 tablet。
- **BUCKETS 8**：本地单 BE 的演示值；生产按数据量调整，桶太小会有小文件/元数据压力，太大则并行度浪费。
- **replication_num=1**：单 BE 必须为 1；生产多 BE 一般设 3。
- **没有分区**：示例为了“插任意日期都能成功”没有建分区。生产建议 `PARTITION BY RANGE(dt)` 按天分区，并开动态分区，查询时用 `dt BETWEEN ...` 做分区裁剪。

其他两种模型什么时候用：

| 模型 | 适合场景 | 例子 |
| --- | --- | --- |
| DUPLICATE | 只追加的日志、事件 | 本示例的订单事件 |
| UNIQUE | 需要按 key 覆盖更新 | 订单主表、用户维表（按 id 去重） |
| AGGREGATE | 预聚合指标 | 按天/渠道预聚合的 GMV、PV |

## 快速开始

### 1. 准备一个 Doris

本地/测试环境按官方 Quick Start 启动 FE + BE：

- <https://doris.apache.org/docs/4.x/getting-started/quick-start/>

确认两个端口通：

```bash
mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT version();"
curl -I http://127.0.0.1:8030
```

> 9030 是 MySQL 协议端口（JDBC 用），8030 是 FE HTTP 端口（Stream Load 用），别配反。

### 2. 建库建表

应用默认会自动建（`demo.doris.init-schema=true`）。也可以手动执行：

```bash
mysql -h 127.0.0.1 -P 9030 -u root < sql/init.sql
mysql -h 127.0.0.1 -P 9030 -u root demo < sql/seed.sql
```

### 3. 启动服务

```bash
# 需要 Java 21
./run.sh
# 或者
mvn spring-boot:run
```

默认连 `127.0.0.1:9030`、库 `demo`、用户 `root`、空密码。改配置用环境变量：

```bash
DORIS_FE_HOST=10.0.0.10 \
DORIS_QUERY_PORT=9030 \
DORIS_HTTP_PORT=8030 \
DORIS_DATABASE=demo \
DORIS_USERNAME=root \
DORIS_PASSWORD=secret \
./run.sh
```

### 4. 验证

```bash
curl http://localhost:8082/api/health
```

正常时返回 200：

```json
{"up":true,"version":"4.1.5","database":"demo","tables":1,"latencyMs":8}
```

Doris 没启动时，应用**仍然能启动**，健康检查返回 503：

```json
{"up":false,"database":"demo","tables":0,"latencyMs":3011,"error":"Connection refused"}
```

这是刻意设计的：连接池 `initializationFailTimeout=-1`，避免 Doris 抖动导致整个应用起不来。

## API 示例

### 单条写入

```bash
curl -X POST http://localhost:8082/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": 2001,
    "userId": 101,
    "productId": 9001,
    "category": "数码",
    "amount": 2999.00,
    "quantity": 1,
    "channel": "douyin",
    "eventTime": "2026-11-20T10:00:00"
  }'
```

返回 `201 Created` 和原对象。`amount=0` 会返回 400：

```json
{"status":400,"error":"Bad Request","message":"参数校验失败","details":{"amount":"amount 至少为 0.01"}}
```

### 批量 JDBC

```bash
curl -X POST http://localhost:8082/api/orders/batch \
  -H 'Content-Type: application/json' \
  -d '{"events":[
    {"eventId":2002,"userId":102,"productId":9002,"category":"家电","amount":899.50,"quantity":1,"channel":"wechat","eventTime":"2026-11-20T11:00:00"},
    {"eventId":2003,"userId":103,"productId":9003,"category":"图书","amount":59.90,"quantity":2,"channel":"organic","eventTime":"2026-11-20T12:00:00"}
  ]}'
```

### Stream Load 批量导入

```bash
curl -X POST http://localhost:8082/api/orders/stream-load \
  -H 'Content-Type: application/json' \
  -d '{"events":[
    {"eventId":2004,"userId":104,"productId":9001,"category":"数码","amount":2999.00,"quantity":1,"channel":"douyin","eventTime":"2026-11-20T13:00:00"}
  ]}'
```

返回：

```json
{"label":"demo_1763..._ab12...","status":"Success","loadedRows":1,"filteredRows":0,"message":"OK"}
```

### 查询与聚合

```bash
curl 'http://localhost:8082/api/orders?page=1&size=10'
curl 'http://localhost:8082/api/orders/2001'
curl 'http://localhost:8082/api/analytics/summary?from=2026-11-18&to=2026-11-20'
curl 'http://localhost:8082/api/analytics/top-products?from=2026-11-18&to=2026-11-20&limit=5'
curl 'http://localhost:8082/api/analytics/daily?from=2026-11-18&to=2026-11-20'
```

## 代码导读（注释在哪）

| 文件 | 重点看什么 |
| --- | --- |
| `DorisProperties` | 每个配置字段的含义；`jdbcUrl()`/`serverJdbcUrl()`/`streamLoadUrl()` 为什么这么拼 |
| `DorisDataSourceConfig` | Hikari 每个参数的作用，尤其是 `initializationFailTimeout=-1` |
| `DorisSchemaInitializer` | 为什么建库要用“不带库名”的 URL；`DUPLICATE KEY` DDL |
| `DorisOrderRepository` | 每条 SQL 的列顺序、`LIMIT/OFFSET`、`COUNT(DISTINCT)` 的代价 |
| `DorisStreamLoadClient` | Stream Load 的 PUT、Basic Auth、`format/jsonpaths/columns/label` 每个头的作用 |
| `OrderService` | 分页边界、批量上限、为什么不用 `@Transactional` |
| `AnalyticsService` | 日期默认值（最近 7 天）和 `from <= to` 校验 |
| `GlobalExceptionHandler` | 400/404/503/500 分别怎么映射 |

### 为什么不用 JPA / MyBatis-Plus

- JPA 没有 Doris 方言，`@GeneratedValue`、`@Version`、事务语义都对不上；
- MyBatis-Plus 能用（底层还是 MySQL 驱动），但 CRUD 自动生成会掩盖 Doris 的分区/分桶/模型设计；
- 教学示例用 `JdbcTemplate` + 显式 SQL，读者能直接看到发往 Doris 的每一条语句。生产项目完全可以在同样的 DataSource 上换成 MyBatis。

### 为什么没有 `@Transactional`

Doris 是 OLAP 引擎，不提供 MySQL 那种跨语句的本地事务。一次 INSERT 或一次 Stream Load 就是一个导入事务；用 `@Transactional` 只会给人“能回滚”的错觉。需要幂等时用 **label**，需要原子多表写入时用 Flink/Spark 作业或两阶段导入。

## JDBC 还是 Stream Load？

| 场景 | 推荐 | 原因 |
| --- | --- | --- |
| 单条/几十条写入 | JDBC INSERT | 简单、低延迟 |
| 几百~几千条 | JDBC batch | 一次网络往返，代码简单 |
| 几万~几十万条 | Stream Load | 高吞吐，FE 直接写 BE，少走查询链路 |
| 持续流式写入 | Flink Doris Connector / Routine Load |  Exactly-once/断点续传 |
| 离线大文件 | Stream Load / Spark Connector | 并行、可重试 |

JDBC 批量插入的坑：

- 单条 INSERT 循环最慢，务必用 `batchUpdate`；
- `rewriteBatchedStatements=true` 会让驱动合并多值 INSERT；
- 一次 batch 不要太大（示例 500 一批），避免 FE 内存和 RPC 压力；
- Doris 的 `INSERT INTO ... VALUES` 适合小批量，大批量走 Stream Load。

Stream Load 的坑：

- **label 幂等**：同一个 label 重复提交，Doris 认为同一事务；本示例每次生成新 label，所以“自动重试”要复用同一个 label，否则会重复写入；
- `format: json` 时 `strip_outer_array=true` 表示最外层是数组；
- `jsonpaths` 与 `columns` 的顺序必须一一对应；
- `Status` 可能是 `Publish Timeout`，表示事务已提交但版本未发布，生产要轮询或重试查询，不能当成功；
- 走 FE 的 8030，不是 BE 的 8040；FE 会转发给 BE。

## 生产化 checklist

- [ ] 账号最小权限：业务账号只给 `SELECT` + `INSERT`，不要用 root；
- [ ] 密码走环境变量/密钥管理，`application.yml` 里不要出现真实密码；
- [ ] 开启 TLS（`useSSL=true` + 证书），不要用 `allowPublicKeyRetrieval=true` 的生产默认；
- [ ] Hikari 最大连接数按 FE 承受能力设置，别把 Doris 当 MySQL OLTP 用；
- [ ] 表按天分区（`PARTITION BY RANGE(dt)`），查询必须带 `dt` 条件；
- [ ] 分桶数按数据量/ BE 数调整，避免过多小 tablet；
- [ ] 多 BE 时 `replication_num=3`；
- [ ] 大导入用 Stream Load，并设置合理 `timeout`、`max_filter_ratio`、`strict_mode`；
- [ ] 监控 `SHOW LOAD`、`SHOW TRANSACTION`、FE/BE 的导入失败率；
- [ ] 分页不要深翻，用游标（`WHERE event_time < ? ORDER BY event_time DESC LIMIT ?`）；
- [ ] `COUNT(DISTINCT)` 在大数据量下很贵，能近似就用 `approx_count_distinct`；
- [ ] 应用启动不要强依赖 Doris（本示例的 `initializationFailTimeout=-1`），用 readinessProbe 判断。

## 测试

```bash
mvn test
```

16 个单测，全部不需要真实 Doris：

- `DorisPropertiesTest`：JDBC / Stream Load URL 拼接；
- `DorisStreamLoadClientTest`：用 JDK `HttpServer` 模拟 FE，断言 PUT、路径、Basic Auth、label、JSON 字段；
- `OrderServiceTest`：分页、批量上限、Stream Load 委托；
- `AnalyticsServiceTest`：默认最近 7 天、日期倒置、limit 边界；
- `DorisHealthServiceTest`：UP / DOWN 两条路径；
- `OrderControllerTest`：201 与 400 的 HTTP 契约。

## 已知边界

- 示例表**没有分区**，方便任意日期写入；生产请按天分区。
- `replication_num=1` 只适合单 BE 本地环境。
- 没有实现 Stream Load 的“同 label 重试”和 `Publish Timeout` 轮询，生产需要补。
- 没有做鉴权/限流；这是数据访问教学服务，不是完整业务系统。
- 本机没有 Docker，未在真实 Doris 上跑过端到端；DDL/驱动/HTTP 头均按 4.1 官方文档编写，单元测试覆盖了 HTTP 协议层。接上 Doris 后如果某条 SQL 报错，优先看 FE 版本和 `SHOW TABLES` 的列类型。
- 本地 Maven 如果无法写 `~/.m2`，可加 `-Dmaven.repo.local=/tmp/m2` 换一个可写仓库。

## 参考

- [Doris MySQL 协议](https://doris.apache.org/docs/4.x/connection-integration/mysql-proto/)
- [Doris Stream Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)
- [Doris 快速开始](https://doris.apache.org/docs/4.x/getting-started/quick-start/)
- [Doris 4.1.5 Release](https://github.com/apache/doris/releases/tag/4.1.5-release)
- [MySQL Connector/J 8.4](https://dev.mysql.com/doc/connector-j/en/)
- [Spring Boot 3.5](https://spring.io/projects/spring-boot)
