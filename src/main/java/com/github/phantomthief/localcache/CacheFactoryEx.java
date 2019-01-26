package com.github.phantomthief.localcache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author w.vela
 */
public interface CacheFactoryEx<T> {

    @Nonnull
    T get(@Nullable T prev) throws Throwable;
}
