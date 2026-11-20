package com.example.doris.domain;

// Bean Validation 注解：由 spring-boot-starter-validation 提供
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

// Java 8 时间类型：比 java.util.Date 更清晰，Jackson 会自动按 ISO-8601 序列化
import java.time.LocalDate;
import java.time.LocalDateTime;

// 金额用 BigDecimal，避免 double 的精度问题
import java.math.BigDecimal;

/**
 * 一条订单事件。
 *
 * <p>用 record 是因为它天然适合“只读的数据载体”：自动生成构造器、getter、equals/hashCode。
 * 每个字段上的注解会在 Controller 收到 JSON 时自动校验。</p>
 *
 * <p>字段命名与 Doris 表的列名一一对应，方便 JDBC 和 Stream Load 复用。</p>
 */
public record OrderEvent(

        // 事件 ID：业务上应全局唯一；这里用 @Positive 保证是正数
        @NotNull(message = "eventId 不能为空")
        @Positive(message = "eventId 必须大于 0")
        Long eventId,

        // 用户 ID：用于按用户去重、统计买家数
        @NotNull(message = "userId 不能为空")
        @Positive(message = "userId 必须大于 0")
        Long userId,

        // 商品 ID
        @NotNull(message = "productId 不能为空")
        @Positive(message = "productId 必须大于 0")
        Integer productId,

        // 商品品类：限制长度，避免超长字符串被 Doris 截断或报错
        @NotBlank(message = "category 不能为空")
        @Size(max = 32, message = "category 最长 32 个字符")
        String category,

        // 订单金额：最小 0.01，最多 10 位整数 + 2 位小数
        @NotNull(message = "amount 不能为空")
        @DecimalMin(value = "0.01", message = "amount 至少为 0.01")
        @Digits(integer = 10, fraction = 2, message = "amount 最多 10 位整数、2 位小数")
        BigDecimal amount,

        // 购买数量
        @NotNull(message = "quantity 不能为空")
        @Positive(message = "quantity 必须大于 0")
        Integer quantity,

        // 来源渠道：例如 douyin、wechat、organic
        @NotBlank(message = "channel 不能为空")
        @Size(max = 16, message = "channel 最长 16 个字符")
        String channel,

        // 事件发生时间：JSON 里用 "2026-11-20T10:30:00" 这种格式
        @NotNull(message = "eventTime 不能为空")
        LocalDateTime eventTime
) {

    /**
     * 从事件时间推导出日期分区列 dt。
     *
     * <p>Doris 里按 dt 过滤可以命中分区裁剪；本示例没建分区，但保留这一列，
     * 以后改成按天分区时不用改表结构。</p>
     *
     * @return 事件所在日期
     */
    public LocalDate dt() {
        // eventTime 已经由校验保证非空，这里可以直接调用
        return eventTime.toLocalDate();
    }
}
