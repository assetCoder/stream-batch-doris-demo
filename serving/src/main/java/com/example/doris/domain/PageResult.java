package com.example.doris.domain;

import java.util.List;

/**
 * 通用分页结果。
 *
 * @param items 当前页数据
 * @param total 总条数
 * @param page  当前页码（从 1 开始）
 * @param size  每页条数
 * @param <T>   元素类型
 */
public record PageResult<T>(
        List<T> items,
        long total,
        int page,
        int size
) {
}
