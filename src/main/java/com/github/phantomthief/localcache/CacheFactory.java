/**
 * 
 */
package com.github.phantomthief.localcache;

/**
 * @author w.vela
 */
public interface CacheFactory<T> {

    public T get() throws Throwable;
}
