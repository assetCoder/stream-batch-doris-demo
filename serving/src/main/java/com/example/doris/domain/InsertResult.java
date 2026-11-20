package com.example.doris.domain;

/**
 * 批量写入的返回结果。
 *
 * @param inserted 实际写入的行数
 */
public record InsertResult(int inserted) {
}
