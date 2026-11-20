package com.example.doris.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 按天聚合的结果。
 *
 * @param dt     日期
 * @param gmv    当天 GMV
 * @param orders 当天订单事件数
 */
public record DailyStat(
        LocalDate dt,
        BigDecimal gmv,
        long orders
) {
}
