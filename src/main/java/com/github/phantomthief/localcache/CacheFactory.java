package com.github.phantomthief.localcache;

import javax.annotation.Nonnull;

/**
 * @author w.vela
 */
public interface CacheFactory<T> {

    /**
     * 注意
     * 1. 本方法不要返回 {@code null}
     * 2. 当构建失败时（本调用抛异常时），会保持上一次构建的结果
     * 3. 当第一次构建异常时，会在caller thread上抛出异常
     */
    @Nonnull
    T get() throws Throwable;
}
