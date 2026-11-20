package com.example.doris.domain;

/**
 * Stream Load 的返回结果。
 *
 * <p>Doris 的响应体里字段是大写开头的（Status、NumberLoadedRows 等），
 * 这里转成 Java 风格，方便前端和其他服务使用。</p>
 *
 * @param label        本次导入的标签；相同 label 重复提交会被 Doris 视为同一事务
 * @param status       Success / Publish Timeout / Fail 等
 * @param loadedRows   成功导入的行数
 * @param filteredRows 被过滤的行数（类型不匹配、strict_mode 等）
 * @param message      Doris 返回的原始消息，排查问题时很有用
 */
public record StreamLoadResult(
        String label,
        String status,
        long loadedRows,
        long filteredRows,
        String message
) {
}
