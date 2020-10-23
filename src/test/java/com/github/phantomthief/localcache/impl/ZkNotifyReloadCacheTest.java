package com.github.phantomthief.localcache.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.localcache.CacheFactory;
import com.github.phantomthief.localcache.CacheFactoryEx;
import com.github.phantomthief.localcache.ReloadableCache;
import com.github.phantomthief.zookeeper.broadcast.ZkBroadcaster;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * 所有 @Disabled 的测试用例都需要手工运行确认
 *
 * @author w.vela
 */
class ZkNotifyReloadCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkNotifyReloadCacheTest.class);

    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

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
        AtomicInteger count = new AtomicInteger();
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> build(count))
                .withNotifyZkPath("/test")
                .withCuratorFactory(() -> curatorFramework)
                .build();
        assertEquals(cache.get(), "0");
        cache.reload();
        sleepUninterruptibly(1, SECONDS);
        assertEquals(cache.get(), "1");
        cache.reloadLocal();
        assertEquals(cache.get(), "2");
    }

    @Disabled
    @Test
    void testScheduled() {
        AtomicInteger count = new AtomicInteger();
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> build(count))
                .enableAutoReload(1, SECONDS)
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
        AtomicInteger count = new AtomicInteger();
        int[] delay = { 1 };
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> build(count))
                .enableAutoReload(() -> ofSeconds(delay[0]++))
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
        zkBroadcaster.subscribe("/myTest", content -> {
            logger.info("received.");
            received.set(content);
        });
        assertNull(received.get());
        zkBroadcaster.broadcast("/myTest", "myContent");
        sleepUninterruptibly(1, SECONDS);
        assertEquals("myContent", received.get());
    }

    @Test
    void testRandomSleep() {
        AtomicInteger count = new AtomicInteger();
        logger.info("test random sleep.");
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> build(count))
                .withNotifyZkPath("/test")
                .withMaxRandomSleepOnNotifyReload(15, SECONDS)
                .withCuratorFactory(() -> curatorFramework)
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

    @Test
    void testDynamicRandomSleep() {
        AtomicInteger count = new AtomicInteger();
        logger.info("test random sleep.");
        long[] max = { 0L };
        LongSupplier maxSleep = () -> max[0];
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> build(count))
                .withNotifyZkPath("/test")
                .withMaxRandomSleepOnNotifyReload(maxSleep)
                .withCuratorFactory(() -> curatorFramework)
                .build();
        max[0] = SECONDS.toMillis(15);
        assertEquals(cache.get(), "0");
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        sleepUninterruptibly(20, SECONDS);
        assertEquals(cache.get(), "1");
        max[0] = SECONDS.toMillis(5);
        logger.info("next round.");
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        cache.reload();
        sleepUninterruptibly(10, SECONDS);
        assertEquals(cache.get(), "2");
    }

    @Test
    void testCacheFactoryWithPrevValue() {
        Long[] data = {null};
        ZkNotifyReloadCache<Long> cache = ZkNotifyReloadCache.<Long> newBuilder()
                .withCacheFactoryEx(prev -> {
                    assertEquals(data[0], prev);
                    if (data[0] == null) {
                        data[0] = 0L;
                    }
                    data[0] ++;
                    return data[0];
                })
                .enableAutoReload(10, TimeUnit.MICROSECONDS)
                .build();
        cache.get();
        sleepUninterruptibly(1, SECONDS);
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
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(factory)
                .withNotifyZkPath("/test")
                .withCuratorFactory(() -> curatorFramework)
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
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(factory)
                .withNotifyZkPath("/test")
                .firstAccessFailFactory(() -> "EMPTY")
                .withCuratorFactory(() -> curatorFramework)
                .build();
        assertEquals("EMPTY", cache.get());
        assertEquals(1, buildCount.get());

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

    // TODO 这个测试用例似乎无法正确运行，需要确认是否something wrong
    @Disabled
    @Test
    @SuppressWarnings("unchecked")
    void testGced() throws Throwable {
        CacheFactory<String> cacheFactory = Mockito.mock(CacheFactory.class);
        doReturn("1").when(cacheFactory).get();
        Runnable recycledListener = Mockito.mock(Runnable.class);
        doNothing().when(recycledListener).run();
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String>newBuilder()
                .withCacheFactory(cacheFactory)
                .firstAccessFailFactory(() -> "EMPTY")
                .withCuratorFactory(() -> curatorFramework)
                .enableAutoReload(() -> Duration.ofMillis(10))
                .onResourceRecycled(recycledListener)
                .build();
        cache.get(); // trigger thread pool creation
        verify(cacheFactory).get();
        Mockito.reset(cacheFactory);
        cache = null;
        System.gc();
        Thread.sleep(100);
        verify(recycledListener).run();
        verify(cacheFactory, never()).get();
    }

    @Test
    void testFirstFailedFactory() {
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .firstAccessFailFactory(() -> "test")
                .withCacheFactory(() -> {
                    throw new Exception();
                })
                .build();
        assertEquals("test", cache.get());
    }

    @Test
    void testFirstFailed() {
        ZkNotifyReloadCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactory(() -> {
                    throw new IOException();
                })
                .build();
        CacheBuildFailedException e = assertThrows(CacheBuildFailedException.class, cache::get);
        assertSame(IOException.class, e.getCause().getClass());
    }

    private void expectedFail(ZkNotifyReloadCache<String> cache) {
        CacheBuildFailedException exception1 = assertThrows(CacheBuildFailedException.class, cache::get);
        assertSame(IOException.class, exception1.getCause().getClass());
    }

    @Test
    void testInterruptFirstFailed() throws Throwable {
        CacheFactoryEx<String> cacheFactory = Mockito.mock(CacheFactoryEx.class);
        Mockito.when(cacheFactory.get(any())).then((Answer<String>) invocation -> {
            Thread.sleep(100);
            throw new Exception();
        }).then((Answer<String>) invocation -> {
            Thread.sleep(100);
            return "test";
        }).then((Answer<String>) invocation -> "shouldNotCalled");

        ReloadableCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactoryEx(cacheFactory)
                .withCuratorFactory(() -> curatorFramework)
                .withInitCacheExecutor(Executors.newCachedThreadPool())
                .build();
        Thread ct = Thread.currentThread();
        Thread t = new Thread(() -> {
            Uninterruptibles.sleepUninterruptibly(50, MILLISECONDS);
            ct.interrupt();
            Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
            ct.interrupt();
        });
        t.setDaemon(true);
        t.start();

        // 验证interrupt的情况下，依然只有一个线程在执行init
        for (int i = 0; i < 10; i++) {
            long waitTime = i * 5;
            Thread st = new Thread(() -> {
                Uninterruptibles.sleepUninterruptibly(waitTime, MILLISECONDS);
                Thread.currentThread().interrupt();
                assertThrows(CancellationException.class, cache::get);
            });
            st.setDaemon(true);
            st.start();
        }

        assertThrows(CancellationException.class, cache::get);
        assertThrows(CacheBuildFailedException.class, cache::get);
        assertThrows(CancellationException.class, cache::get);
        assertEquals("test", cache.get());
        assertEquals("test", cache.get());
        Uninterruptibles.sleepUninterruptibly(200, MILLISECONDS);
        Mockito.verify(cacheFactory, times(2)).get(any());
    }

    @Test
    void testInterruptDirect() throws Throwable {
        // 默认使用direct executor，行为和原来一直，总是抛出CacheBuildFailedException，并且构建不成功
        CacheFactoryEx<String> cacheFactory = Mockito.mock(CacheFactoryEx.class);
        Mockito.when(cacheFactory.get(any())).then((Answer<String>) invocation -> {
            Thread.sleep(100);
            return "test1";
        }).then((Answer<String>) invocation -> {
            Thread.sleep(100);
            return "test2";
        }).then((Answer<String>) invocation -> "test3");

        ReloadableCache<String> cache = ZkNotifyReloadCache.<String> newBuilder()
                .withCacheFactoryEx(cacheFactory)
                .withCuratorFactory(() -> curatorFramework)
                .build();
        Thread ct = Thread.currentThread();
        Thread t = new Thread(() -> {
            Uninterruptibles.sleepUninterruptibly(50, MILLISECONDS);
            ct.interrupt();
            Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
            ct.interrupt();
        });
        t.setDaemon(true);
        t.start();

        assertThrows(CacheBuildFailedException.class, cache::get);
        Uninterruptibles.sleepUninterruptibly(60, MILLISECONDS);
        assertThrows(CacheBuildFailedException.class, cache::get);
        Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
        assertEquals("test3", cache.get());
    }

    private String build(AtomicInteger count) {
        return String.valueOf(count.getAndIncrement());
    }
}
