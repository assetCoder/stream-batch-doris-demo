package com.example.doris.service;

// Spring 的服务层注解
import org.springframework.stereotype.Service;

// 领域对象
import com.example.doris.domain.OrderEvent;
import com.example.doris.domain.PageResult;
import com.example.doris.domain.StreamLoadResult;
// 数据访问层
import com.example.doris.repository.DorisOrderRepository;
// Stream Load 客户端
import com.example.doris.client.DorisStreamLoadClient;

// 集合与可选值
import java.util.List;
import java.util.Optional;

/**
 * 订单写入与查询的业务层。
 *
 * <p>Controller 只负责 HTTP 参数和状态码，真正的规则（分页边界、批量上限、
 * 走 JDBC 还是 Stream Load）放在这里，方便复用和单测。</p>
 */
@Service
public class OrderService {

    // 单次 JDBC 批量插入的最大条数，与 OrderBatchRequest 的校验保持一致
    private static final int MAX_BATCH_SIZE = 5000;

    // 分页每页最大条数，防止一次拉太多数据把内存打满
    private static final int MAX_PAGE_SIZE = 200;

    // 数据访问层
    private final DorisOrderRepository repository;

    // Stream Load 客户端
    private final DorisStreamLoadClient streamLoadClient;

    /**
     * 构造器注入：依赖由 Spring 传入，便于单元测试时替换成 Mock。
     *
     * @param repository       订单仓储
     * @param streamLoadClient Stream Load 客户端
     */
    public OrderService(DorisOrderRepository repository, DorisStreamLoadClient streamLoadClient) {
        this.repository = repository;
        this.streamLoadClient = streamLoadClient;
    }

    /**
     * 写入一条订单。
     *
     * @param event 已通过 Bean Validation 的订单事件
     * @return 原样返回，方便调用方拿到 eventId
     */
    public OrderEvent create(OrderEvent event) {
        // 单条写入走 JDBC INSERT，简单直接
        repository.insert(event);
        // 返回事件对象，Controller 用 201 Created 响应
        return event;
    }

    /**
     * 批量写入（JDBC batch）。
     *
     * @param events 订单事件列表
     * @return 实际写入行数
     */
    public int batchCreate(List<OrderEvent> events) {
        // 空列表直接返回 0，不访问数据库
        if (events == null || events.isEmpty()) {
            return 0;
        }
        // 超过上限时拒绝，避免单次请求过大
        if (events.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("单次最多 " + MAX_BATCH_SIZE + " 条");
        }
        // 交给 Repository 的 batchUpdate
        return repository.batchInsert(events);
    }

    /**
     * 通过 Stream Load 批量导入。
     *
     * <p>适合几千到几十万行的场景；比 JDBC batch 少很多网络往返。</p>
     *
     * @param events 订单事件列表
     * @return Doris 的导入结果
     */
    public StreamLoadResult streamLoad(List<OrderEvent> events) {
        // 空列表没有意义，提前拦截
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("events 不能为空");
        }
        // 上限与批量接口保持一致
        if (events.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("单次最多 " + MAX_BATCH_SIZE + " 条");
        }
        // 委托给客户端；真正的 HTTP 细节在 DorisStreamLoadClient 里
        return streamLoadClient.load(events);
    }

    /**
     * 按 eventId 查询。
     *
     * @param eventId 事件 ID
     * @return 可能为空
     */
    public Optional<OrderEvent> find(long eventId) {
        // 直接透传 Repository 的结果
        return repository.findByEventId(eventId);
    }

    /**
     * 分页查询。
     *
     * @param page 页码，从 1 开始
     * @param size 每页条数，1~200
     * @return 当前页数据和总数
     */
    public PageResult<OrderEvent> page(int page, int size) {
        // 页码至少为 1
        if (page < 1) {
            throw new IllegalArgumentException("page 必须大于等于 1");
        }
        // 每页条数限制在 1~200
        if (size < 1 || size > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("size 必须在 1~" + MAX_PAGE_SIZE + " 之间");
        }

        // 用 long 计算偏移量，避免 page 很大时 int 溢出
        long offset = (long) (page - 1) * size;
        // Doris 的 OFFSET 太大时性能很差；这里限制在 int 范围内并给出明确错误
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("page 过大，请使用游标分页");
        }

        // 先查当前页数据
        List<OrderEvent> items = repository.page((int) offset, size);
        // 再查总数；两条 SQL 之间可能有新数据写入，分页总数允许有轻微误差
        long total = repository.count();
        // 组装成通用分页结果
        return new PageResult<>(items, total, page, size);
    }
}
