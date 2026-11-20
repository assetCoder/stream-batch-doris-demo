# Flink + Spark + Doris 流批一体示例

> 一个仓库把三条链路串起来：**Flink 实时写入 Doris → Spark 离线算 ADS → Spring Boot 查询 Doris**。

这不是把三个引擎塞进一个 JVM，而是让它们各司其职：

```text
                    ┌─────────────────────────────┐
 Kafka(order-events)│  Flink 2.2（实时）           │
 ──────────────────▶│  JSON 解析 / 时间转换 / 脏数据过滤 │
                    └──────────────┬──────────────┘
                                   │ Stream Load / Doris Connector
                                   ▼
                    ┌─────────────────────────────┐
                    │ Doris 4.1（统一存储 + 服务）  │
                    │  order_events   DUPLICATE KEY│
                    │  ads_daily_channel UNIQUE KEY│
                    └───────┬───────────────▲─────┘
                            │               │
              Spark 4.1 读 DWD          Spring Boot / BI 查 ADS
              （离线聚合/回补）              （高并发点查/报表）
```

## 目录

```text
stream-batch-doris-demo/
├── flink-job/                 Flink SQL 作业（Kafka → Doris）
│   ├── src/main/java/com/example/streaming/
│   │   ├── JobConfig.java          配置 + SQL 生成，逐行注释
│   │   └── OrderStreamingJob.java  作业入口
│   └── src/test/java/...           3 个 SQL/配置单测
├── spark-job/                 Spark 4.1 离线作业（Doris → Doris）
│   ├── spark_ads_job.py            读 DWD、算 ADS、写 Doris
│   ├── submit.sh                   spark-submit 封装
│   └── tests/test_transform.py     2 个聚合逻辑测试
├── serving/                   Spring Boot 服务（复用 doris-demo）
│   └── src/...                     新增 /api/analytics/channel-daily
├── producer/                  Kafka 模拟数据生产者
├── infra/
│   ├── doris/init.sql             分层建表 DDL
│   └── kafka/create-topic.sh      创建 3 分区 topic
├── data/sample_order_events.json  本地 dry-run 数据
└── docker-compose.yml         Kafka + Flink（Doris 用现有集群）
```

## 版本矩阵

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| Apache Doris | **4.1.5** | FE 9030（MySQL 协议）、8030（HTTP/Stream Load） |
| Flink | **2.2.1** | 没有选 2.3：Kafka connector 最新只到 `5.0.0-2.2`，版本对齐优先 |
| Flink Doris Connector | **26.3.0**（`flink-doris-connector-2.2`） | 与 Flink 2.2 对齐 |
| Flink Kafka Connector | **5.0.0-2.2** | 与 Flink 2.2 对齐 |
| Spark | **4.1.4** | 官方 Doris connector 只提供 `spark-4.1` 构件 |
| Spark Doris Connector | **26.1.0**（`spark-doris-connector-spark-4.1`） | Spark 4.0 没有对应 connector，需用 JDBC 回退 |
| Spring Boot | **3.5.7** | Java 21 |
| MySQL Connector/J | **8.4.0** | Spring Boot/JDBC 连接 Doris FE |

> 版本矩阵比“每个都选最新”重要。Flink 2.3 + Spark 4.3 看起来新，但 Kafka/Spark Doris connector 未必有对应构件，混用会直接 `NoSuchMethodError`。

## 技术选型参照

> 原文：[引擎选型对照：Hive/Spark/Flink/OLAP](https://docs.qiqicto.com/dwh/engine-selection.html)
>
> 说明：抓取时该页面返回 **502 Bad Gateway**，暂时无法核对全文。下面先按页面主题，把“选型维度 + 本项目的取舍”整理成可复用的检查表；等原站恢复后，再对照原文补充/修正结论。

### 一张表看懂四类引擎

| 维度 | Hive | Spark | Flink | Doris |
| --- | --- | --- | --- | --- |
| 核心定位 | 离线数仓 / SQL on Hadoop | 通用批处理、ETL、ML | 有状态流处理 | MPP OLAP / 统一查询服务 |
| 典型延迟 | 分钟 ~ 小时 | 秒 ~ 分钟（微批） | 毫秒 ~ 秒 | 亚秒 ~ 秒 |
| 吞吐/规模 | PB 级，吞吐高 | PB 级，吞吐高 | TB ~ PB，状态大 | TB ~ 百 TB，单集群 |
| 数据更新 | 分区重写 | 分区重写 / 湖表 | 流式 upsert | Unique Key upsert |
| 高并发点查 | 不擅长 | 一般 | 不擅长 | 强项 |
| 事务/一致性 | 弱 | 弱（靠湖表） | checkpoint + 2PC | 导入事务 + label |
| 运维成本 | Hadoop 体系重 | Spark 集群 | Flink 集群 + 状态 | FE/BE + compaction |
| 最适合 | T+1 离线报表 | 回补、复杂 Join、ML | 实时 ETL、风控、告警 | 实时明细 + 报表服务 |

### 本项目为什么这样选

- **Flink 做实时接入**：Kafka 订单事件需要低延迟清洗、时间转换、脏数据过滤；Flink 的 checkpoint + Doris Connector 2PC 能保证“至少一次/精确一次”的导入语义。
- **Spark 做离线聚合/回补**：日/渠道 ADS 需要可重跑、可全量重算；Spark 的批处理吞吐和 SQL 生态更合适，读 Doris DWD 后按 `dt` 重算。
- **Doris 做存储与服务**：明细和 ADS 都落在 Doris，Spring Boot/BI 只查 Doris，避免把结果再同步回 MySQL；Unique Key 让 Spark 重跑天然幂等。
- **Hive 没有进主链路**：示例数据量小；真实项目里 Hive/S3/Parquet 更适合放冷数据，Spark 读湖、算完只把结果写 Doris，避免大扫描打爆 Doris。
- **不用 Spark 做实时**：微批延迟和持续运行成本不适合秒级链路；**不用 Flink 做全量回补**：状态和吞吐不如 Spark 批任务直接。
- **不让 Flink/Spark 互调**：两者通过 Doris 表交接，任何一方重跑都不影响另一方。

### 选型时按顺序问 8 个问题

1. **延迟 SLA**：秒级/分钟级/T+1？先排除不满足的引擎。
2. **数据量级与保留周期**：TB 还是 PB？决定 Doris 单集群是否够，还是需要湖仓。
3. **更新语义**：只追加、按主键 upsert，还是按分区覆盖？对应 Duplicate/Unique/重写分区。
4. **查询并发**：BI 高并发点查选 Doris；大扫描/ML 选 Spark。
5. **计算复杂度**：复杂 Join、窗口、CEP 分别更适合 Spark/Flink。
6. **团队与运维**：有没有 Flink/Spark/Doris 的运维能力，比“引擎参数”更影响落地。
7. **生态与连接器**：Flink/Spark 与 Doris 的 connector 是否与引擎大版本对齐（本项目就因此选 Flink 2.2 而非 2.3）。
8. **失败恢复**：Flink checkpoint、Spark 重跑、Doris label 幂等，三者要能组合成可恢复的链路。

### 持续学习建议

读原文时，建议对照本项目逐条验证：

- 原文对 Hive/Spark/Flink/OLAP 的边界条件，是否适用于你的数据量；
- 原文的选型结论是否假设了某种存储/调度（HDFS、Kafka、YARN/K8s）；
- 把“延迟、吞吐、并发、成本、运维”换成你团队的真实数字，再重跑一遍决策；
- 每引入一个新引擎，先问：**它负责哪一层、谁写哪张表、失败后怎么重跑**。

相关官方文档：

- [Doris Flink 导入](https://doris.apache.org/docs/4.x/data-operate/import/data-source/flink/)
- [Doris Spark Connector](https://doris.apache.org/docs/4.x/connection-integration/data-integration/spark-doris-connector/)
- [Flink 2.2 文档](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
- [Spark 4.1 文档](https://spark.apache.org/docs/4.1.4/)

## 数据分层与写入方

| 表 | 写入方 | 模型 | 说明 |
| --- | --- | --- | --- |
| `demo.order_events` | Flink | `DUPLICATE KEY(event_id, user_id)` | 明细/ODS，只追加 |
| `demo.ads_daily_channel` | Spark | `UNIQUE KEY(dt, channel)` | 日/渠道 ADS，重跑覆盖 |

**每张表只有一个写入方**。不要让 Flink 和 Spark 同时写 `ads_daily_channel`，否则口径和幂等会打架。

## 快速开始

### 0. 前置条件

- Java 21、Maven 3.9+
- Python 3.10+（Spark 4.1 需要）
- 一个可连的 Doris 4.1（FE 9030/8030）
- Docker（可选，用来起 Kafka/Flink；Doris 不在 compose 里）

### 1. 初始化 Doris

```bash
mysql -h 127.0.0.1 -P 9030 -u root < infra/doris/init.sql
```

`serving` 启动时也会自动建这两张表（`demo.doris.init-schema=true`），二选一即可。

### 2. 构建并测试 Flink 作业

```bash
cd flink-job
mvn test
mvn package
cd ..
```

产物：`flink-job/target/flink-order-job-1.0.0.jar`（已 shade 连接器，70 MB 左右）。

### 3. 启动 Kafka 并建 topic

```bash
docker compose up -d kafka
./infra/kafka/create-topic.sh
```

宿主机 producer 用 `127.0.0.1:29092`；Flink 容器内用 `kafka:9092`。

### 4. 提交 Flink 作业

方式 A：Docker Compose（Flink 2.2.1 镜像）

```bash
docker compose up -d jobmanager taskmanager

# Mac/Windows 用 host.docker.internal 访问宿主机 Doris
DORIS_FENODES=host.docker.internal:8030 \
DORIS_USERNAME=root \
DORIS_PASSWORD= \
docker compose up -d flink-submit
```

方式 B：已有 Flink 集群

```bash
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export DORIS_FENODES=127.0.0.1:8030
export DORIS_USERNAME=root
export DORIS_PASSWORD=

flink run -d \
  -c com.example.streaming.OrderStreamingJob \
  flink-job/target/flink-order-job-1.0.0.jar
```

Flink UI：<http://localhost:8085>（compose 把容器 8081 映射到宿主机 8085，避开已有端口）。

### 5. 生产模拟数据

```bash
python3 -m venv .venv-producer
.venv-producer/bin/pip install -r producer/requirements.txt

.venv-producer/bin/python producer/produce_orders.py \
  --bootstrap 127.0.0.1:29092 \
  --topic order-events \
  --count 20
```

也可以直接用 Kafka 自带命令行灌 `data/order_events.jsonl`：

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic order-events < data/order_events.jsonl
```

### 6. 跑 Spark 离线聚合

```bash
python3 -m venv .venv-spark
.venv-spark/bin/pip install -r spark-job/requirements.txt
source .venv-spark/bin/activate

# 用 Doris connector 读取 + 写入（推荐）
./spark-job/submit.sh --fenodes 127.0.0.1:8030 --dt 2026-11-20
```

`submit.sh` 会自动带上：

```text
org.apache.doris:spark-doris-connector-spark-4.1:26.1.0
```

没有 connector 时用 JDBC 回退（需要 MySQL 驱动）：

```bash
spark-submit \
  --packages com.mysql:mysql-connector-j:8.4.0 \
  spark-job/spark_ads_job.py \
  --mode jdbc --fenodes 127.0.0.1:8030 --dt 2026-11-20
```

**不连 Doris 的本地验证**：

```bash
.venv-spark/bin/python spark-job/spark_ads_job.py \
  --dry-run \
  --input data/sample_order_events.json
```

输出：

```text
+----------+-------+-------+------+------+---------------+
|dt        |channel|gmv    |orders|buyers|avg_buyer_value|
+----------+-------+-------+------+------+---------------+
|2026-11-20|douyin |3058.90|2     |1     |3058.90        |
|2026-11-20|wechat |899.50 |1     |1     |899.50         |
...
```

### 7. 启动 Spring Boot 查询服务

```bash
cd serving
./run.sh
```

```bash
curl 'http://localhost:8082/api/health'
curl 'http://localhost:8082/api/orders?page=1&size=10'
curl 'http://localhost:8082/api/analytics/channel-daily?from=2026-11-20&to=2026-11-21'
```

`channel-daily` 只查 Spark 产出的 ADS 表；`summary/top-products/daily` 查 Flink 写入的明细表。

## 端到端验证顺序

```text
1. Doris 建表
2. Flink 启动，checkpoint 正常
3. Kafka 有数据，Flink UI 的 records received/sent 增长
4. Doris: SELECT COUNT(*) FROM demo.order_events;
5. Spark 跑完，Doris: SELECT * FROM demo.ads_daily_channel;
6. Spring Boot: GET /api/analytics/channel-daily
```

Doris 侧排查 SQL：

```sql
SHOW LOAD FROM demo ORDER BY CreateTime DESC LIMIT 5;
SELECT COUNT(*) FROM demo.order_events;
SELECT * FROM demo.ads_daily_channel ORDER BY dt DESC, channel LIMIT 10;
```

## 关键设计

### 为什么 Flink 用 Table API，而不是手写 Stream Load

Doris Flink Connector 已经封装了 Stream Load、label、重试和 2PC。SQL 里只要写：

```sql
WITH (
  'connector' = 'doris',
  'fenodes' = 'fe:8030',
  'table.identifier' = 'demo.order_events',
  'sink.label-prefix' = 'flink-orders-',
  'sink.enable-2pc' = 'true'
)
```

就同时获得了 **checkpoint 触发的两阶段提交** 和 **失败重试**。手写 HTTP 客户端适合学习协议，不适合生产重复造轮子。

### 为什么 Spark 读 Doris，而不是读 Kafka

- Spark 是离线/回补引擎，读 Kafka 会引入 offset 管理、重复消费、批流边界问题；
- Doris 里的 DWD 已经是清洗后的稳定快照，Spark 直接读它，逻辑简单、可重跑；
- 数据量大时，Spark 应读 Hive/S3/Parquet 冷数据，只把结果写 Doris，避免把 Doris 当数据湖扫。

### 为什么 ADS 用 Unique Key

`UNIQUE KEY(dt, channel)` 让 Spark 重跑同一天同一渠道时是 **upsert**，而不是追加重复行。`append` 模式即可，不需要先 `DELETE`。

### 为什么 Flink 和 Spark 不直接调用

它们是独立集群，直接 RPC 会把调度、失败重试、版本耦合在一起。用 Doris 表做契约，任何一方重跑都不影响另一方。

## 生产化清单

- [ ] `order_events` 按 `dt` 做 RANGE 分区，查询必须带 `dt`；示例为方便未分区；
- [ ] `replication_num=3`（多 BE），示例单 BE 用 1；
- [ ] Flink 开 checkpoint + 2PC，并监控 checkpoint 失败/aligned 时间；
- [ ] Stream Load label 可重试、可查询；不要每次重试都换 label；
- [ ] Spark 任务按 `dt` 分区覆盖，避免全表重算；
- [ ] 监控 Doris `SHOW LOAD`、compaction score、tablet 数量；
- [ ] Flink/Spark 资源与 Doris 查询错峰，避免导入打满 BE；
- [ ] 密码走 Secret，不要写进 compose/README；
- [ ] 生产禁用 `auto.create.topics`，topic 分区数提前规划；
- [ ] 时区统一 `Asia/Shanghai`，Doris `DATETIME` 不要和 `TIMESTAMP` 混用。

## 测试与验证情况

已验证：

- `flink-job`：`mvn test` **3 个单测通过**；`mvn package` 成功生成 fat jar；jar 内包含 Kafka/Doris connector，不包含 Flink core。
- `spark-job`：`pytest` **2 个聚合测试通过**；`--dry-run` 用本地 JSON 跑出正确结果。
- `serving`：`mvn test` **16 个单测通过**（含新增的 ADS 查询委托测试）。

未验证（受本机环境限制）：

- 本机没有 Docker，也没有可连的 Doris/Kafka，因此 **没有跑真实端到端**；
- `docker-compose.yml` 的 Flink/Kafka 镜像 tag、Flink 容器到宿主 Doris 的网络需要按你的环境确认；
- Flink SQL 的 connector 参数按 Doris 4.1 官方文档和 connector 26.3.0 的 option 列表编写，但未在真实 FE 上执行 `CREATE TABLE`/导入。
- Spark 本地 dry-run/单测已在 PySpark **4.1.4** 上跑通；真实 Doris connector 模式仍未连真实 FE。

如果你有可连的 Doris/Kafka，把地址给我，我可以把这三段真实跑通并修掉版本差异。

## 参考

- [Doris Flink 导入](https://doris.apache.org/docs/4.x/data-operate/import/data-source/flink/)
- [Doris Spark Connector](https://doris.apache.org/docs/4.x/connection-integration/data-integration/spark-doris-connector/)
- [Doris Stream Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)
- [Doris 4.1.5 Release](https://github.com/apache/doris/releases/tag/4.1.5-release)
- [Flink 2.2.1](https://archive.apache.org/dist/flink/flink-2.2.1/)
