package com.example.doris.repository;

// Spring 的仓储注解：语义上表示“数据访问层”，同时会被组件扫描注册
import org.springframework.stereotype.Repository;

// JdbcTemplate：Spring 对 JDBC 的轻量封装，负责拿连接、设参数、关资源、转异常
import org.springframework.jdbc.core.JdbcTemplate;
// RowMapper：把 ResultSet 的一行映射成一个 Java 对象
import org.springframework.jdbc.core.RowMapper;

// SQL 类型
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Statement;
import java.sql.Timestamp;
// 返回可选值，避免 null
import java.util.Optional;
// 集合
import java.util.List;

// 领域对象
import com.example.doris.domain.ChannelDailyStat;
import com.example.doris.domain.DailyStat;
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.OrderSummary;
import com.example.doris.domain.ProductStat;

/**
 * 订单明细的 Doris 数据访问层。
 *
 * <p>这里全部使用显式 SQL，而不是 JPA/Hibernate。原因：</p>
 * <ul>
 *   <li>Doris 是 OLAP 引擎，SQL 写法（分区、分桶、聚合）比 ORM 的自动生成更重要；</li>
 *   <li>JPA 的方言不支持 Doris 的 DUPLICATE/UNIQUE/AGGREGATE 模型；</li>
 *   <li>显式 SQL 更容易在 DBA 那里 review，也更容易做性能优化。</li>
 * </ul>
 */
@Repository
public class DorisOrderRepository {

    // 所有列的顺序必须与 INSERT 的占位符顺序一致
    private static final String COLUMNS =
            "event_id, user_id, product_id, category, amount, quantity, channel, event_time, dt";

    // 单条插入：9 个列对应 9 个问号
    private static final String INSERT_SQL =
            "INSERT INTO order_events (" + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    // 查询基础语句；后面的方法会拼接 WHERE / ORDER BY / LIMIT
    private static final String SELECT_SQL =
            "SELECT " + COLUMNS + " FROM order_events";

    // 把一行 ResultSet 转成 OrderEvent；字段名与表列名一致
    private static final RowMapper<OrderEvent> ROW_MAPPER = (rs, rowNum) -> new OrderEvent(
            // BIGINT -> Long
            rs.getLong("event_id"),
            // BIGINT -> Long
            rs.getLong("user_id"),
            // INT -> Integer
            rs.getInt("product_id"),
            // VARCHAR -> String
            rs.getString("category"),
            // DECIMAL -> BigDecimal，保留精度
            rs.getBigDecimal("amount"),
            // INT -> Integer
            rs.getInt("quantity"),
            // VARCHAR -> String
            rs.getString("channel"),
            // DATETIME -> Timestamp -> LocalDateTime
            rs.getTimestamp("event_time").toLocalDateTime()
    );

    // JdbcTemplate 由 Spring Boot 自动配置，构造器注入
    private final JdbcTemplate jdbc;

    /**
     * 构造器注入。
     *
     * @param jdbc Spring 提供的 JDBC 模板
     */
    public DorisOrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 插入一条订单事件。
     *
     * @param event 订单事件
     * @return 受影响的行数（Doris 通常返回 1）
     */
    public int insert(OrderEvent event) {
        // 参数顺序必须与 INSERT_SQL 中的列顺序完全一致
        return jdbc.update(INSERT_SQL,
                event.eventId(),                       // 1. event_id
                event.userId(),                        // 2. user_id
                event.productId(),                     // 3. product_id
                event.category(),                      // 4. category
                event.amount(),                        // 5. amount
                event.quantity(),                      // 6. quantity
                event.channel(),                       // 7. channel
                Timestamp.valueOf(event.eventTime()),  // 8. DATETIME
                Date.valueOf(event.dt())               // 9. DATE
        );
    }

    /**
     * 批量插入。
     *
     * <p>Doris 更适合“大批量、少批次”的写入方式：一次几百到几千条，
     * 比循环单条 INSERT 快很多。真正上百万行建议用 Stream Load。</p>
     *
     * @param events 订单事件列表
     * @return 成功写入的行数
     */
    public int batchInsert(List<OrderEvent> events) {
        // batchUpdate 会把同一 SQL 复用 PreparedStatement，并按 batchSize 分批发送
        int[][] result = jdbc.batchUpdate(INSERT_SQL, events, 500, (ps, event) -> {
            // 这里的下标从 1 开始，对应 SQL 里的第 1~9 个问号
            ps.setLong(1, event.eventId());
            ps.setLong(2, event.userId());
            ps.setInt(3, event.productId());
            ps.setString(4, event.category());
            ps.setBigDecimal(5, event.amount());
            ps.setInt(6, event.quantity());
            ps.setString(7, event.channel());
            ps.setTimestamp(8, Timestamp.valueOf(event.eventTime()));
            ps.setDate(9, Date.valueOf(event.dt()));
        });

        // 统计实际影响行数；驱动可能返回 SUCCESS_NO_INFO(-2)，这时按 1 行估算
        int count = 0;
        for (int[] batch : result) {
            for (int rows : batch) {
                if (rows > 0) {
                    count += rows;
                } else if (rows == Statement.SUCCESS_NO_INFO) {
                    count += 1;
                }
            }
        }
        return count;
    }

    /**
     * 按事件 ID 查一条。
     *
     * <p>Doris 的 DUPLICATE 模型允许相同 key 存在多行，所以加 LIMIT 1，
     * 避免同一个 event_id 被重复导入时返回多条。</p>
     *
     * @param eventId 事件 ID
     * @return 可能为空的订单事件
     */
    public Optional<OrderEvent> findByEventId(long eventId) {
        // LIMIT 1 既避免重复行，也能让查询尽快返回
        List<OrderEvent> rows = jdbc.query(
                SELECT_SQL + " WHERE event_id = ? LIMIT 1",
                ROW_MAPPER,
                eventId
        );
        // 列表为空时返回 Optional.empty()，调用方不用处理 null
        return rows.stream().findFirst();
    }

    /**
     * 分页查询，按事件时间倒序。
     *
     * <p>注意：Doris 的 OFFSET 越大越慢，生产环境建议用“游标分页”
     * （例如 WHERE event_time < 上一页最后一条的时间）。</p>
     *
     * @param offset 跳过多少行
     * @param size   每页多少行
     * @return 当前页数据
     */
    public List<OrderEvent> page(int offset, int size) {
        // offset/size 已在 Service 层校验为非负整数，这里直接拼接；
        // 部分 Doris 版本对 LIMIT 占位符支持不完整，用整数拼接更稳。
        String sql = SELECT_SQL
                + " ORDER BY event_time DESC, event_id DESC"
                + " LIMIT " + size + " OFFSET " + offset;
        // RowMapper 负责把每一行转成对象
        return jdbc.query(sql, ROW_MAPPER);
    }

    /**
     * 统计总行数。
     *
     * @return 订单事件总数
     */
    public long count() {
        // COUNT(*) 在 Doris 里会扫描明细；数据量大时可以用 SHOW DATA 或汇总表替代
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM order_events", Long.class);
        // queryForObject 理论上不会返回 null，这里做一次兜底
        return total == null ? 0L : total;
    }

    /**
     * 统计时间范围内的 GMV、订单数、买家数、客单价。
     *
     * @param from 开始日期（含）
     * @param to   结束日期（含）
     * @return 汇总指标
     */
    public OrderSummary summary(Date from, Date to) {
        // COALESCE 处理“范围内没有数据”时 SUM 返回 NULL 的情况
        // COUNT(DISTINCT user_id) 是精确去重，数据量大时会吃内存；
        // 如果只要近似值，Doris 提供 approx_count_distinct()
        String sql = """
                SELECT COALESCE(SUM(amount), 0) AS gmv,
                       COUNT(*)                 AS order_count,
                       COUNT(DISTINCT user_id)  AS buyer_count
                FROM order_events
                WHERE dt BETWEEN ? AND ?
                """;

        // queryForObject 的第二参数是 RowMapper，第三、四个参数是 SQL 占位符
        return jdbc.queryForObject(sql, (rs, rowNum) -> {
            BigDecimal gmv = rs.getBigDecimal("gmv");
            long orders = rs.getLong("order_count");
            long buyers = rs.getLong("buyer_count");
            // 客单价 = GMV / 订单数；订单为 0 时返回 0，避免除零
            BigDecimal avg = orders == 0
                    ? BigDecimal.ZERO
                    : gmv.divide(BigDecimal.valueOf(orders), 2, RoundingMode.HALF_UP);
            return new OrderSummary(gmv, orders, buyers, avg);
        }, from, to);
    }

    /**
     * 商品维度的 Top N。
     *
     * @param from  开始日期（含）
     * @param to    结束日期（含）
     * @param limit 返回条数（1~100）
     * @return 按 GMV 倒序的商品列表
     */
    public List<ProductStat> topProducts(Date from, Date to, int limit) {
        // GROUP BY 的列必须出现在 SELECT 里；ORDER BY 可以使用别名 gmv
        String sql = """
                SELECT product_id,
                       category,
                       SUM(amount) AS gmv,
                       COUNT(*)    AS order_count
                FROM order_events
                WHERE dt BETWEEN ? AND ?
                GROUP BY product_id, category
                ORDER BY gmv DESC
                LIMIT %d
                """.formatted(limit);

        // 逐行映射成 ProductStat
        return jdbc.query(sql, (rs, rowNum) -> new ProductStat(
                rs.getInt("product_id"),
                rs.getString("category"),
                rs.getBigDecimal("gmv"),
                rs.getLong("order_count")
        ), from, to);
    }

    /**
     * 按天聚合 GMV 和订单数。
     *
     * @param from 开始日期（含）
     * @param to   结束日期（含）
     * @return 按日期升序的列表
     */
    public List<DailyStat> daily(Date from, Date to) {
        // dt 是 DATE 类型，可以直接 GROUP BY
        String sql = """
                SELECT dt,
                       SUM(amount) AS gmv,
                       COUNT(*)    AS order_count
                FROM order_events
                WHERE dt BETWEEN ? AND ?
                GROUP BY dt
                ORDER BY dt
                """;

        // java.sql.Date -> LocalDate，方便 Jackson 输出 "2026-11-20"
        return jdbc.query(sql, (rs, rowNum) -> new DailyStat(
                rs.getDate("dt").toLocalDate(),
                rs.getBigDecimal("gmv"),
                rs.getLong("order_count")
        ), from, to);
    }
    /**
     * 查询 Spark 产出的日/渠道 ADS 指标。
     *
     * <p>ADS 表是 Unique Key(dt, channel)，Spark 重跑同一天时覆盖，不会产生重复行。</p>
     *
     * @param from 开始日期（含）
     * @param to   结束日期（含）
     * @return 按日期、渠道排序的指标列表
     */
    public List<ChannelDailyStat> channelDaily(Date from, Date to) {
        String sql = """
                SELECT dt,
                       channel,
                       gmv,
                       orders,
                       buyers,
                       avg_buyer_value
                FROM ads_daily_channel
                WHERE dt BETWEEN ? AND ?
                ORDER BY dt, channel
                """;

        return jdbc.query(sql, (rs, rowNum) -> new ChannelDailyStat(
                rs.getDate("dt").toLocalDate(),
                rs.getString("channel"),
                rs.getBigDecimal("gmv"),
                rs.getLong("orders"),
                rs.getLong("buyers"),
                rs.getBigDecimal("avg_buyer_value")
        ), from, to);
    }

}
