package com.example.doris.web;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.doris.domain.ChannelDailyStat;
import com.example.doris.domain.DailyStat;
import com.example.doris.domain.OrderSummary;
import com.example.doris.domain.ProductStat;
import com.example.doris.service.AnalyticsService;

import java.time.LocalDate;
import java.util.List;

/**
 * 分析类 HTTP 接口。
 *
 * <p>日期参数用 ISO 格式（yyyy-MM-dd），例如：</p>
 * <pre>
 * GET /api/analytics/summary?from=2026-11-01&amp;to=2026-11-20
 * </pre>
 */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    // 分析业务层
    private final AnalyticsService service;

    /**
     * 构造器注入。
     *
     * @param service 分析服务
     */
    public AnalyticsController(AnalyticsService service) {
        this.service = service;
    }

    /**
     * 汇总指标。
     *
     * @param from 开始日期，可省略
     * @param to   结束日期，可省略
     * @return GMV / 订单数 / 买家数 / 客单价
     */
    @GetMapping("/summary")
    public OrderSummary summary(
            // required=false 表示可以不传；@DateTimeFormat 让 Spring 按 yyyy-MM-dd 解析
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        // 默认范围和先后校验在 AnalyticsService 里
        return service.summary(from, to);
    }

    /**
     * 商品 Top N。
     *
     * @param from  开始日期，可省略
     * @param to    结束日期，可省略
     * @param limit 返回条数，默认 10
     * @return 商品聚合列表
     */
    @GetMapping("/top-products")
    public List<ProductStat> topProducts(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(defaultValue = "10") int limit) {
        // limit 的边界由 Service 校验
        return service.topProducts(from, to, limit);
    }

    /**
     * 按天趋势。
     *
     * @param from 开始日期，可省略
     * @param to   结束日期，可省略
     * @return 每天的 GMV 和订单数
     */
    @GetMapping("/daily")
    public List<DailyStat> daily(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        // 交给 Service 处理默认范围
        return service.daily(from, to);
    }
    /**
     * 日/渠道指标（Spark 离线作业写入 ADS 表）。
     *
     * @param from 开始日期，可省略
     * @param to   结束日期，可省略
     * @return 按日期、渠道的 GMV/订单/买家/客单价
     */
    @GetMapping("/channel-daily")
    public List<ChannelDailyStat> channelDaily(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return service.channelDaily(from, to);
    }

}
