package com.example.doris.domain;

/**
 * Doris 健康检查结果。
 *
 * <p>应用启动成功不代表 Doris 可用，所以单独提供一个探活接口，
 * 方便本地调试、Kubernetes readinessProbe 或监控系统使用。</p>
 *
 * @param up        是否连通
 * @param version   Doris 版本号（SELECT version()）
 * @param database  当前数据库
 * @param tables    当前库里的表数量
 * @param latencyMs 探活耗时（毫秒）
 * @param error     失败原因；成功时为 null
 */
public record HealthStatus(
        boolean up,
        String version,
        String database,
        int tables,
        long latencyMs,
        String error
) {
}
