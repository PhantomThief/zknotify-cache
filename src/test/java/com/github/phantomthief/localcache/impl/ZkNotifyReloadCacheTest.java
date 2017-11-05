package com.github.phantomthief.localcache.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 */
class ZkNotifyReloadCacheTest {

    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;
    private static AtomicInteger count = new AtomicInteger();

    @BeforeAll
    static void init() throws Exception {
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new ExponentialBackoffRetry(10000, 20));
        curatorFramework.start();
    }

    @AfterAll
    static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }

    @Test
    void test() {
        count.set(0);
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(this::build) //
                .withNotifyZkPath("/test") //
                .withCuratorFactory(() -> curatorFramework) //
                .build();
        assertEquals(cache.get(), "0");
        cache.reload();
        sleepUninterruptibly(1, SECONDS);
        assertEquals(cache.get(), "1");
    }

    @Test
    void testScheduled() {
        count.set(0);
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(this::build) //
                .enableAutoReload(1, SECONDS) //
                .build();
        assertEquals(cache.get(), "0");
        sleepUninterruptibly(1300, MILLISECONDS);
        assertEquals(cache.get(), "1");
        sleepUninterruptibly(1300, MILLISECONDS);
        assertEquals(cache.get(), "2");
    }

    private String build() {
        return String.valueOf(count.getAndIncrement());
    }
}
