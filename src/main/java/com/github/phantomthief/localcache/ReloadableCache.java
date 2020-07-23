package com.github.phantomthief.localcache;

import java.util.function.Supplier;

/**
 * @author w.vela
 */
public interface ReloadableCache<T> extends Supplier<T> {

    /**
     * 当第一次构建失败时，本方法会上抛异常
     */
    @Override
    T get();

    /**
     * 通知全局缓存更新
     * 注意：如果本地缓存没有初始化，本方法并不会初始化本地缓存并重新加载
     *
     * 如果需要初始化本地缓存，请先调用 {@link ReloadableCache#get()}
     */
    void reload();

    /**
     * 更新本地缓存的本地副本
     * 注意：如果本地缓存没有初始化，本方法并不会初始化并刷新本地的缓存
     *
     * 如果需要初始化本地缓存，请先调用 {@link ReloadableCache#get()}
     */
    void reloadLocal();
}
