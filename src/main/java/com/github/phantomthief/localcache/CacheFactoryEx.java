package com.github.phantomthief.localcache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author w.vela
 */
public interface CacheFactoryEx<T> {

    /**
     * 注意
     * 1. 本方法不要返回 {@code null}
     * 2. 当构建失败时（本调用抛异常时），会保持上一次构建的结果
     * 3. 当第一次构建异常时，会在caller thread上抛出异常
     *
     * @param prev 上一次缓存的值，如果第一次构建为 {@code null}
     */
    @Nonnull
    T get(@Nullable T prev) throws Throwable;
}
