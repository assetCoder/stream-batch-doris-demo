package com.example.doris.service;

import org.springframework.stereotype.Service;

import com.example.doris.domain.ChannelDailyStat;
import com.example.doris.domain.DailyStat;
import com.example.doris.domain.OrderSummary;
import com.example.doris.domain.ProductStat;
import com.example.doris.repository.DorisOrderRepository;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

/**
 * 分析查询的业务层。
 *
 * <p>负责“日期范围默认值”和“边界校验”，Repository 只关心 SQL。</p>
 */
@Service
public class AnalyticsService {

    // 数据访问层
    private final DorisOrderRepository repository;

    /**
     * 构造器注入。
     *
     * @param repository 订单仓储
     */
    public AnalyticsService(DorisOrderRepository repository) {
        this.repository = repository;
    }

    /**
     * 汇总指标。
     *
     * @param from 开始日期，可为 null（默认最近 7 天）
     * @param to   结束日期，可为 null（默认今天）
     * @return GMV、订单数、买家数、客单价
     */
    public OrderSummary summary(LocalDate from, LocalDate to) {
        // 统一处理默认值和先后顺序
        Range range = resolveRange(from, to);
        // java.time.LocalDate 转成 JDBC 的 java.sql.Date
        return repository.summary(Date.valueOf(range.from()), Date.valueOf(range.to()));
    }

    /**
     * 商品 Top N。
     *
     * @param from  开始日期，可为 null
     * @param to    结束日期，可为 null
     * @param limit 返回条数，1~100
     * @return 商品聚合列表
     */
    public List<ProductStat> topProducts(LocalDate from, LocalDate to, int limit) {
        // Top N 的 N 不能太大，避免一次返回过多数据
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("limit 必须在 1~100 之间");
        }
        // 复用同一套日期范围规则
        Range range = resolveRange(from, to);
        // 交给 Repository 执行 GROUP BY + ORDER BY + LIMIT
        return repository.topProducts(Date.valueOf(range.from()), Date.valueOf(range.to()), limit);
    }

    /**
     * 按天趋势。
     *
     * @param from 开始日期，可为 null
     * @param to   结束日期，可为 null
     * @return 按日期升序的聚合列表
     */
    public List<DailyStat> daily(LocalDate from, LocalDate to) {
        // 复用同一套日期范围规则
        Range range = resolveRange(from, to);
        // Repository 内部 GROUP BY dt
        return repository.daily(Date.valueOf(range.from()), Date.valueOf(range.to()));
    }

    /**
     * 计算实际查询的日期范围。
     *
     * <p>规则：</p>
     * <ul>
     *   <li>from、to 都为空：最近 7 天（含今天）；</li>
     *   <li>只给 to：从 to 往前推 6 天；</li>
     *   <li>只给 from：到 from 往后推 6 天；</li>
     *   <li>都给了：必须 from <= to。</li>
     * </ul>
     *
     * @param from 用户传入的开始日期
     * @param to   用户传入的结束日期
     * @return 校验后的范围
     */
    private Range resolveRange(LocalDate from, LocalDate to) {
        // 结束日期默认今天
        LocalDate end = to != null ? to : LocalDate.now();
        // 开始日期默认“结束日期往前 6 天”，一共 7 天
        LocalDate start = from != null ? from : end.minusDays(6);

        // 只给 from 时，让范围仍然保持 7 天，而不是 start~今天
        if (from != null && to == null) {
            end = from.plusDays(6);
        }

        // 开始日期不能晚于结束日期
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("from 不能晚于 to");
        }

        // 返回不可变的 Range
        return new Range(start, end);
    }

    /**
     * 内部的日期范围值对象。
     *
     * @param from 开始日期（含）
     * @param to   结束日期（含）
     */
    private record Range(LocalDate from, LocalDate to) {
    }
    /**
     * 日/渠道 ADS 指标（Spark 离线作业产出）。
     *
     * @param from 开始日期，可为 null
     * @param to   结束日期，可为 null
     * @return 按日期、渠道排序的指标列表
     */
    public List<ChannelDailyStat> channelDaily(LocalDate from, LocalDate to) {
        Range range = resolveRange(from, to);
        return repository.channelDaily(Date.valueOf(range.from()), Date.valueOf(range.to()));
    }

}
