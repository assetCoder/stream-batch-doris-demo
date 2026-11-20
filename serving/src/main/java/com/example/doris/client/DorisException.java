package com.example.doris.client;

/**
 * Doris 访问失败时抛出的业务异常。
 *
 * <p>继承 RuntimeException，避免每个方法都声明 throws；
 * Controller 层的 GlobalExceptionHandler 会把它转成 503。</p>
 */
public class DorisException extends RuntimeException {

    // HTTP 状态码；不是 HTTP 错误时可以是 0
    private final int httpStatus;

    // Doris 返回的原始响应，排查导入失败时非常有用
    private final String responseBody;

    /**
     * 只带消息的构造器。
     *
     * @param message 错误说明
     */
    public DorisException(String message) {
        this(message, 0, null);
    }

    /**
     * 带 HTTP 状态和响应体的构造器。
     *
     * @param message      错误说明
     * @param httpStatus   HTTP 状态码
     * @param responseBody Doris 原始响应
     */
    public DorisException(String message, int httpStatus, String responseBody) {
        // 把状态码拼进异常消息，日志里一眼能看到
        super(httpStatus > 0 ? message + "（HTTP " + httpStatus + "）" : message);
        this.httpStatus = httpStatus;
        this.responseBody = responseBody;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getResponseBody() {
        return responseBody;
    }
}
