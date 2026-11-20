package com.example.doris.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 日/渠道 ADS 指标，由 Spark 离线作业写入。
 *
 * @param dt            日期
 * @param channel       渠道
 * @param gmv           GMV
 * @param orders        订单数
 * @param buyers        买家数
 * @param avgBuyerValue 客单价（GMV / 买家数）
 */
public record ChannelDailyStat(
        LocalDate dt,
        String channel,
        BigDecimal gmv,
        long orders,
        long buyers,
        BigDecimal avgBuyerValue
) {
}
