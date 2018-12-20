package com.github.phantomthief.localcache.impl;

/**
 * @author w.vela
 * Created on 2018-12-20.
 */
class CacheBuildFailedException extends RuntimeException {

    CacheBuildFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
