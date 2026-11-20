package com.example.doris.service;

import com.example.doris.domain.OrderSummary;
import com.example.doris.repository.DorisOrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AnalyticsService 的日期范围与边界测试。
 */
@ExtendWith(MockitoExtension.class)
class AnalyticsServiceTest {

    @Mock
    private DorisOrderRepository repository;

    @InjectMocks
    private AnalyticsService service;

    @Test
    void summaryShouldDefaultToLastSevenDays() {
        // 固定“今天”，避免测试里两次调用 now() 跨天
        LocalDate today = LocalDate.now();
        OrderSummary expected = new OrderSummary(new BigDecimal("100.00"), 2, 2, new BigDecimal("50.00"));
        when(repository.summary(Date.valueOf(today.minusDays(6)), Date.valueOf(today)))
                .thenReturn(expected);

        OrderSummary actual = service.summary(null, null);

        assertThat(actual).isEqualTo(expected);
        // 验证默认范围确实是“今天往前 6 天 ~ 今天”
        verify(repository).summary(Date.valueOf(today.minusDays(6)), Date.valueOf(today));
    }

    @Test
    void summaryShouldRejectReversedRange() {
        LocalDate from = LocalDate.of(2026, 11, 20);
        LocalDate to = LocalDate.of(2026, 11, 1);

        assertThatThrownBy(() -> service.summary(from, to))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from");
    }

    @Test
    void topProductsShouldRejectInvalidLimit() {
        // limit 必须是 1~100
        assertThatThrownBy(() -> service.topProducts(null, null, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit");

        assertThatThrownBy(() -> service.topProducts(null, null, 101))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit");
    }

    @Test
    void dailyWithOnlyFromShouldUseSevenDayWindow() {
        LocalDate from = LocalDate.of(2026, 11, 1);
        LocalDate to = from.plusDays(6);

        // 只传 from 时，范围是 from ~ from+6
        service.daily(from, null);

        verify(repository).daily(Date.valueOf(from), Date.valueOf(to));
    }
    @Test
    void channelDailyShouldDelegateWithResolvedRange() {
        LocalDate from = LocalDate.of(2026, 11, 20);
        LocalDate to = LocalDate.of(2026, 11, 21);
        when(repository.channelDaily(Date.valueOf(from), Date.valueOf(to))).thenReturn(List.of());

        assertThat(service.channelDaily(from, to)).isEmpty();
        verify(repository).channelDaily(Date.valueOf(from), Date.valueOf(to));
    }

}
