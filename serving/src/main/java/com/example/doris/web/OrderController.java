package com.example.doris.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.example.doris.domain.InsertResult;
import com.example.doris.domain.OrderBatchRequest;
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.PageResult;
import com.example.doris.domain.StreamLoadResult;
import com.example.doris.service.OrderService;

import jakarta.validation.Valid;

/**
 * 订单相关的 HTTP 接口。
 *
 * <p>Controller 的职责很薄：接参数、调 Service、决定状态码。
 * 业务规则都在 OrderService 里，方便单测和复用。</p>
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    // 业务层
    private final OrderService service;

    /**
     * 构造器注入。
     *
     * @param service 订单服务
     */
    public OrderController(OrderService service) {
        this.service = service;
    }

    /**
     * 新增一条订单。
     *
     * <p>POST /api/orders</p>
     *
     * @param event 请求体，@Valid 会触发 OrderEvent 上的校验注解
     * @return 201 Created + 原订单
     */
    @PostMapping
    public ResponseEntity<OrderEvent> create(@Valid @RequestBody OrderEvent event) {
        // 调用 Service 写入 Doris
        OrderEvent saved = service.create(event);
        // 201 表示“资源已创建”，比 200 更语义化
        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }

    /**
     * JDBC 批量新增。
     *
     * <p>POST /api/orders/batch</p>
     *
     * @param request 批量请求（包含 events 列表）
     * @return 实际写入行数
     */
    @PostMapping("/batch")
    public InsertResult batch(@Valid @RequestBody OrderBatchRequest request) {
        // 批量写入，返回成功行数
        int inserted = service.batchCreate(request.events());
        // 包装成 JSON：{"inserted": 100}
        return new InsertResult(inserted);
    }

    /**
     * Stream Load 批量导入。
     *
     * <p>POST /api/orders/stream-load</p>
     *
     * @param request 批量请求（包含 events 列表）
     * @return Doris 的导入结果
     */
    @PostMapping("/stream-load")
    public StreamLoadResult streamLoad(@Valid @RequestBody OrderBatchRequest request) {
        // 走 HTTP 8030 的 Stream Load，而不是 JDBC
        return service.streamLoad(request.events());
    }

    /**
     * 按 eventId 查询。
     *
     * <p>GET /api/orders/{eventId}</p>
     *
     * @param eventId 事件 ID
     * @return 200 + 订单；不存在时 404
     */
    @GetMapping("/{eventId}")
    public OrderEvent get(@PathVariable long eventId) {
        // find 返回 Optional；为空时抛 404，由 Spring 转成标准错误响应
        return service.find(eventId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "eventId 不存在：" + eventId));
    }

    /**
     * 分页查询。
     *
     * <p>GET /api/orders?page=1&size=20</p>
     *
     * @param page 页码，默认 1
     * @param size 每页条数，默认 20
     * @return 当前页数据 + 总数
     */
    @GetMapping
    public PageResult<OrderEvent> page(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size) {
        // 边界校验在 Service 里做，Controller 只透传
        return service.page(page, size);
    }
}
