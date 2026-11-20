package com.example.doris.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * 批量请求的外层包装。
 *
 * <p>为什么不直接收 {@code List<OrderEvent>}？<br>
 * Spring MVC 对“顶层集合”不会自动级联校验元素，包一层 DTO 后，
 * {@code List<@Valid OrderEvent>} 才能让每个元素都触发校验。</p>
 *
 * @param events 订单事件列表
 */
public record OrderBatchRequest(

        // 至少一条；最多 5000 条，避免单次请求体过大把 FE 打满
        @NotEmpty(message = "events 不能为空")
        @Size(max = 5000, message = "单次最多 5000 条")
        List<@Valid OrderEvent> events
) {
}
