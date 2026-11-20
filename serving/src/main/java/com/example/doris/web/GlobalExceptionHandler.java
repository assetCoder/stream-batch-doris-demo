package com.example.doris.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import com.example.doris.client.DorisException;
import com.example.doris.domain.ApiError;

import jakarta.validation.ConstraintViolationException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全局异常处理。
 *
 * <p>把不同异常翻译成统一的 JSON 错误体，避免把堆栈直接暴露给调用方。</p>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    // 日志对象：只对 5xx 打 error，4xx 打 warn 即可
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Bean Validation 失败（@Valid 校验不通过）。
     *
     * @param e 校验异常
     * @return 400 + 字段级错误
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException e) {
        // 用 LinkedHashMap 保持字段出现顺序，前端展示更稳定
        Map<String, String> details = new LinkedHashMap<>();
        // 一个字段可能有多个错误，只保留第一条
        for (FieldError fieldError : e.getBindingResult().getFieldErrors()) {
            details.putIfAbsent(fieldError.getField(), fieldError.getDefaultMessage());
        }
        // 返回 400，details 里是 {"amount": "amount 至少为 0.01"}
        return build(HttpStatus.BAD_REQUEST, "参数校验失败", details);
    }

    /**
     * 请求体不是合法 JSON，或类型无法转换。
     *
     * @param e 解析异常
     * @return 400
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadable(HttpMessageNotReadableException e) {
        // 不回显原始请求体，避免日志/响应里出现敏感数据
        return build(HttpStatus.BAD_REQUEST, "请求体不是合法 JSON，或字段类型不匹配", null);
    }

    /**
     * 方法参数上的约束校验失败（例如 @RequestParam 上的注解）。
     *
     * @param e 约束异常
     * @return 400 + 参数级错误
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiError> handleConstraint(ConstraintViolationException e) {
        Map<String, String> details = new LinkedHashMap<>();
        // 遍历每个违反约束的参数
        e.getConstraintViolations().forEach(v ->
                details.putIfAbsent(v.getPropertyPath().toString(), v.getMessage()));
        return build(HttpStatus.BAD_REQUEST, "参数校验失败", details);
    }

    /**
     * 业务参数不合法（Service 层主动抛出）。
     *
     * @param e 非法参数异常
     * @return 400
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegalArgument(IllegalArgumentException e) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage(), null);
    }

    /**
     * Doris 访问失败（网络、权限、导入失败等）。
     *
     * @param e Doris 异常
     * @return 503，提示调用方稍后重试
     */
    @ExceptionHandler(DorisException.class)
    public ResponseEntity<ApiError> handleDoris(DorisException e) {
        // 记录 warn，带上响应体方便排查；响应体可能较大，截断后记录
        log.warn("Doris 调用失败：{}", e.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "Doris 访问失败：" + e.getMessage(), null);
    }

    /**
     * Spring 把 JDBC 异常包装成 DataAccessException。
     *
     * @param e 数据访问异常
     * @return 503
     */
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiError> handleDataAccess(DataAccessException e) {
        // 取最内层消息，通常包含“Connection refused”“Unknown database”等关键信息
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        log.warn("Doris 数据访问失败：{}", root.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "Doris 数据访问失败：" + root.getMessage(), null);
    }

    /**
     * Controller 主动抛出的状态异常（例如 404）。
     *
     * @param e 状态异常
     * @return 对应状态码
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> handleResponseStatus(ResponseStatusException e) {
        // 从异常里取出 HTTP 状态码
        HttpStatus status = HttpStatus.resolve(e.getStatusCode().value());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        // reason 可能为空，给一个兜底文案
        String reason = e.getReason() == null ? status.getReasonPhrase() : e.getReason();
        return build(status, reason, null);
    }

    /**
     * 未预期的异常：记录堆栈，对外只返回通用错误。
     *
     * @param e 任意异常
     * @return 500
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnknown(Exception e) {
        // error 级别记录完整堆栈，便于排查
        log.error("未处理的异常", e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "服务器内部错误", null);
    }

    /**
     * 构造统一错误响应。
     *
     * @param status  HTTP 状态
     * @param message 给人看的说明
     * @param details 字段级错误，可为 null
     * @return ResponseEntity
     */
    private ResponseEntity<ApiError> build(HttpStatus status, String message, Map<String, String> details) {
        // ApiError 里的 timestamp 用 UTC，避免不同机器时区不一致
        ApiError body = new ApiError(
                Instant.now(),
                status.value(),
                status.getReasonPhrase(),
                message,
                details
        );
        return ResponseEntity.status(status).body(body);
    }
}
