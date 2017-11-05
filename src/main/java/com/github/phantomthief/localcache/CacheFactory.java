package com.github.phantomthief.localcache;

import javax.annotation.Nonnull;

/**
 * @author w.vela
 */
public interface CacheFactory<T> {

    @Nonnull
    T get() throws Throwable;
}
