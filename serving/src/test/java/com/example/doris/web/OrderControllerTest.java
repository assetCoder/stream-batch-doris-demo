package com.example.doris.web;

import com.example.doris.domain.OrderEvent;
import com.example.doris.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Controller 层契约测试：只启动 Web 层，Service 用 Mock 替换。
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    // Spring 提供的 MockMvc，用来发“假 HTTP 请求”
    @Autowired
    private MockMvc mvc;

    // 替换真实的 OrderService，避免测试依赖 Doris
    @MockitoBean
    private OrderService service;

    @Test
    void createShouldReturn201AndBody() throws Exception {
        // Service 原样返回传入的事件
        when(service.create(any(OrderEvent.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        // 合法的 JSON 请求
        String json = """
                {
                  "eventId": 1,
                  "userId": 101,
                  "productId": 9001,
                  "category": "数码",
                  "amount": 2999.00,
                  "quantity": 1,
                  "channel": "douyin",
                  "eventTime": "2026-11-20T10:00:00"
                }
                """;

        mvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventId").value(1))
                .andExpect(jsonPath("$.amount").value(2999.00));
    }

    @Test
    void createShouldReturn400WhenAmountIsInvalid() throws Exception {
        // amount=0 会触发 @DecimalMin("0.01")
        String json = """
                {
                  "eventId": 1,
                  "userId": 101,
                  "productId": 9001,
                  "category": "数码",
                  "amount": 0,
                  "quantity": 1,
                  "channel": "douyin",
                  "eventTime": "2026-11-20T10:00:00"
                }
                """;

        mvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details.amount").exists());
    }
}
