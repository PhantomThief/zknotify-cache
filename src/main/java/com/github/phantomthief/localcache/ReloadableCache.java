package com.github.phantomthief.localcache;

import java.util.function.Supplier;

/**
 * @author w.vela
 */
public interface ReloadableCache<T> extends Supplier<T> {

    void reload();

    void reloadLocal();
}
