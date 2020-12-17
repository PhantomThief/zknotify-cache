package com.github.phantomthief.localcache.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.localcache.ReloadableCache;

/**
 * @author w.vela
 * Created on 2020-12-17.
 */
class TestLoopInit {

    private final ReloadableCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
            .withCacheFactoryEx(this::factory)
            .build();

    private String factory(@Nullable String old) {
        cache.get();
        return "test";
    }

    @Test
    void test() {
        // TODO 这里要不要在 DCL 里做一下重入检测，让失败来的更快一些呢？
        assertThrows(StackOverflowError.class, cache::get);
    }
}
