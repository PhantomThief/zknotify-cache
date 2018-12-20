package com.github.phantomthief.localcache.impl;

/**
 * @author w.vela
 * Created on 2018-12-20.
 */
class CachedObject<T> {

    final T obj;
    final Throwable e;

    CachedObject(T obj) {
        this(obj, null);
    }

    CachedObject(T obj, Throwable e) {
        this.obj = obj;
        this.e = e;
    }
}
