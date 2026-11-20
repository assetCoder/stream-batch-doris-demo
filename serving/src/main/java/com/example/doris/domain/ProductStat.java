package com.example.doris.domain;

import java.math.BigDecimal;

/**
 * 商品维度的聚合结果。
 *
 * @param productId 商品 ID
 * @param category  商品品类
 * @param gmv       该商品 GMV
 * @param orders    该商品订单事件数
 */
public record ProductStat(
        int productId,
        String category,
        BigDecimal gmv,
        long orders
) {
}
