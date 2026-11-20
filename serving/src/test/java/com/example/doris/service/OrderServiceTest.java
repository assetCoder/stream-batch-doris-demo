package com.example.doris.service;

import com.example.doris.client.DorisStreamLoadClient;
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.PageResult;
import com.example.doris.domain.StreamLoadResult;
import com.example.doris.repository.DorisOrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * OrderService 的边界校验测试。
 *
 * <p>用 Mockito 替换 Repository 和 Stream Load 客户端，不依赖真实 Doris。</p>
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    // 模拟数据访问层
    @Mock
    private DorisOrderRepository repository;

    // 模拟 Stream Load 客户端
    @Mock
    private DorisStreamLoadClient streamLoadClient;

    // 把上面两个 Mock 注入 OrderService 的构造器
    @InjectMocks
    private OrderService service;

    @Test
    void pageShouldPassOffsetAndReturnTotal() {
        // Repository 返回空页和总数 3
        when(repository.page(0, 20)).thenReturn(List.of());
        when(repository.count()).thenReturn(3L);

        PageResult<OrderEvent> result = service.page(1, 20);

        // 第 1 页的 offset 应该是 0
        verify(repository).page(0, 20);
        assertThat(result.total()).isEqualTo(3);
        assertThat(result.page()).isEqualTo(1);
    }

    @Test
    void pageShouldRejectInvalidArguments() {
        // 页码从 1 开始
        assertThatThrownBy(() -> service.page(0, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page");

        // size 上限 200
        assertThatThrownBy(() -> service.page(1, 201))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }

    @Test
    void batchShouldRejectTooManyRows() {
        // 构造 5001 条，超过服务端上限
        List<OrderEvent> events = Collections.nCopies(5001, sampleEvent());
        assertThatThrownBy(() -> service.batchCreate(events))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("5000");
    }

    @Test
    void streamLoadShouldDelegateToClient() {
        // 客户端返回成功结果
        StreamLoadResult expected = new StreamLoadResult("label-1", "Success", 1, 0, "OK");
        when(streamLoadClient.load(List.of(sampleEvent()))).thenReturn(expected);

        StreamLoadResult actual = service.streamLoad(List.of(sampleEvent()));

        // 必须原样返回客户端的结果
        assertThat(actual).isEqualTo(expected);
        verify(streamLoadClient).load(List.of(sampleEvent()));
    }

    private OrderEvent sampleEvent() {
        // 生成一条合法订单，供多个测试复用
        return new OrderEvent(1L, 101L, 9001, "数码", new BigDecimal("2999.00"), 1, "douyin",
                LocalDateTime.of(2026, 11, 20, 10, 0));
    }
}
