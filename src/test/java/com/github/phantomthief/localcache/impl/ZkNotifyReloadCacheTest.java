package com.github.phantomthief.localcache.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.broadcast.ZkBroadcaster;

/**
 * @author w.vela
 */
class ZkNotifyReloadCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkNotifyReloadCacheTest.class);

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

    @Test
    void testNotify() {
        ZkBroadcaster zkBroadcaster = new ZkBroadcaster(() -> curatorFramework);
        AtomicReference<String> received = new AtomicReference<>();
        zkBroadcaster.subscribe("/myTest", () -> {
            logger.info("received.");
            received.set("test");
        });
        assertNull(received.get());
        zkBroadcaster.broadcast("/myTest", "myContent");
        sleepUninterruptibly(1, SECONDS);
        assertEquals("test", received.get());
    }

    @Test
    void testRandomSleep() {
        count.set(0);
        logger.info("test random sleep.");
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(this::build) //
                .withNotifyZkPath("/test") //
                .withMaxRandomSleepOnNotifyReload(15, SECONDS) //
                .withCuratorFactory(() -> curatorFramework) //
                .build();
        assertEquals(cache.get(), "0");
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        sleepUninterruptibly(20, SECONDS);
        assertEquals(cache.get(), "1");
        logger.info("next round.");
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        sleepUninterruptibly(20, SECONDS);
        assertEquals(cache.get(), "2");
    }

    private String build() {
        return String.valueOf(count.getAndIncrement());
    }
}
