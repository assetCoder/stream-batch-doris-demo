package com.example.doris.domain;

import java.time.Instant;
import java.util.Map;

/**
 * 统一的错误响应体。
 *
 * @param timestamp 发生时间（UTC）
 * @param status    HTTP 状态码
 * @param error     简短错误类型，例如 Bad Request
 * @param message   给调用方看的说明
 * @param details   字段级错误，例如 {"amount": "amount 至少为 0.01"}
 */
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        Map<String, String> details
) {
}
