package com.example.doris.domain;

import java.math.BigDecimal;

/**
 * 一段时间内的汇总指标。
 *
 * @param gmv           成交总额（GMV）
 * @param orders        订单事件数
 * @param buyers        去重买家数
 * @param avgOrderValue 客单价 = GMV / 订单数，保留 2 位小数
 */
public record OrderSummary(
        BigDecimal gmv,
        long orders,
        long buyers,
        BigDecimal avgOrderValue
) {
}
