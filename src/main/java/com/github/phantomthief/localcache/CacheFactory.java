/**
 * 
 */
package com.github.phantomthief.localcache;

/**
 * @author w.vela
 */
public interface CacheFactory<T> {

    T get() throws Throwable;
}
