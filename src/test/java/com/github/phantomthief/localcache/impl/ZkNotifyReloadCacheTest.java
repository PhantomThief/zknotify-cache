package com.github.phantomthief.localcache.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.localcache.CacheFactory;
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

    @Disabled
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

    @Disabled
    @Test
    void testDynamicScheduled() {
        count.set(0);
        int[] delay = { 1 };
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(this::build) //
                .enableAutoReload(() -> ofSeconds(delay[0]++)) //
                .build();
        assertEquals(cache.get(), "0");
        sleepUninterruptibly(1300, MILLISECONDS);
        assertEquals(cache.get(), "1");
        sleepUninterruptibly(2300, MILLISECONDS);
        assertEquals(cache.get(), "2");
        sleepUninterruptibly(3300, MILLISECONDS);
        assertEquals(cache.get(), "3");
        sleepUninterruptibly(4300, MILLISECONDS);
        assertEquals(cache.get(), "4");
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

    @Disabled
    @Test
    void testFirstAccessException() throws InterruptedException {
        AtomicInteger buildCount = new AtomicInteger();
        boolean[] exception = { true };
        CacheFactory<String> factory = () -> {
            int count = buildCount.incrementAndGet();
            logger.info("building...{}", count);
            if (exception[0]) {
                throw new IOException("my test");
            } else {
                return "OK" + count;
            }
        };
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(factory) //
                .withNotifyZkPath("/test") //
                .withCuratorFactory(() -> curatorFramework) //
                .build();
        expectedFail(cache);
        assertEquals(1, buildCount.get());
        expectedFail(cache);
        assertEquals(2, buildCount.get());

        cache.reload(); // didn't reload actually.
        SECONDS.sleep(20);

        assertEquals(2, buildCount.get());
        expectedFail(cache);
        assertEquals(3, buildCount.get());
        expectedFail(cache);
        assertEquals(4, buildCount.get());

        cache.reload();
        SECONDS.sleep(20); // didn't reload actually.

        exception[0] = false;
        assertEquals(4, buildCount.get());

        assertEquals("OK5", cache.get());
        assertEquals(5, buildCount.get());
        assertEquals("OK5", cache.get());
        assertEquals(5, buildCount.get());

        cache.reload();
        SECONDS.sleep(20); // reload occurred.

        assertEquals(6, buildCount.get());
        assertEquals("OK6", cache.get());

        exception[0] = true;

        cache.reload();
        SECONDS.sleep(20); // reload occurred.

        assertEquals(7, buildCount.get());
        assertEquals("OK6", cache.get()); // last value
    }

    @Disabled
    @Test
    void testFirstExceptionWithFirstCacheFactory() throws InterruptedException {
        AtomicInteger buildCount = new AtomicInteger();
        boolean[] exception = { true };
        CacheFactory<String> factory = () -> {
            int count = buildCount.incrementAndGet();
            logger.info("building...{}", count);
            if (exception[0]) {
                throw new IOException("my test");
            } else {
                return "OK" + count;
            }
        };
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder() //
                .withCacheFactory(factory) //
                .withNotifyZkPath("/test") //
                .firstAccessFailFactory(() -> "EMPTY") //
                .withCuratorFactory(() -> curatorFramework) //
                .build();
        assertEquals("EMPTY", cache.get());
        assertEquals(1, buildCount.get());

        cache.reload();
        SECONDS.sleep(20);

        assertEquals(2, buildCount.get());
        assertEquals("EMPTY", cache.get());

        exception[0] = false;

        cache.reload();
        SECONDS.sleep(20);

        assertEquals(3, buildCount.get());
        assertEquals("OK3", cache.get());
    }

    private void expectedFail(ZkNotifyReloadCache<String> cache) {
        CacheBuildFailedException exception1 = assertThrows(CacheBuildFailedException.class, cache::get);
        assertSame(IOException.class, exception1.getCause().getClass());
    }

    private String build() {
        return String.valueOf(count.getAndIncrement());
    }
}
