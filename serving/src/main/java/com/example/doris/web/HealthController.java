package com.example.doris.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.doris.domain.HealthStatus;
import com.example.doris.service.DorisHealthService;

/**
 * 健康检查接口。
 *
 * <p>和其他业务接口分开：探活只需要这一条，K8s 的 readinessProbe 可以直接指向它。</p>
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    // 健康检查服务
    private final DorisHealthService service;

    /**
     * 构造器注入。
     *
     * @param service 健康检查服务
     */
    public HealthController(DorisHealthService service) {
        this.service = service;
    }

    /**
     * 探活。
     *
     * @return Doris 正常时 200，不可用时 503
     */
    @GetMapping("/health")
    public ResponseEntity<HealthStatus> health() {
        // 执行探活；失败不会抛异常，而是 up=false
        HealthStatus status = service.check();
        // 用 HTTP 状态码表达健康状态，方便负载均衡/监控直接判断
        HttpStatus httpStatus = status.up() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(httpStatus).body(status);
    }
}
