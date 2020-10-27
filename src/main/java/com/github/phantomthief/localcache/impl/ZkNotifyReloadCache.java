package com.github.phantomthief.localcache.impl;

import static com.github.phantomthief.concurrent.MoreFutures.scheduleWithDynamicDelay;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.MIN_PRIORITY;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;

import com.github.phantomthief.localcache.CacheFactory;
import com.github.phantomthief.localcache.CacheFactoryEx;
import com.github.phantomthief.localcache.ReloadableCache;
import com.github.phantomthief.zookeeper.broadcast.Broadcaster;
import com.github.phantomthief.zookeeper.broadcast.ZkBroadcaster;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class ZkNotifyReloadCache<T> implements ReloadableCache<T> {

    private static final Logger logger = getLogger(ZkNotifyReloadCache.class);

    private final CacheFactoryEx<T> cacheFactory;
    private final Supplier<T> firstAccessFailFactory;
    private final Set<String> notifyZkPaths;
    private final Consumer<T> oldCleanup;
    private final LongSupplier maxRandomSleepOnNotifyReload;
    private final Broadcaster broadcaster;
    private final Supplier<Duration> scheduleRunDuration;
    @Nullable
    private final ScheduledExecutorService executor;
    private final Runnable recycleListener;
    private Future<?> postInitFuture;

    private volatile T cachedObject;

    private ZkNotifyReloadCache(Builder<T> builder) {
        this.cacheFactory = builder.cacheFactory;
        this.firstAccessFailFactory = wrapTry(builder.firstAccessFailFactory);
        this.notifyZkPaths = builder.notifyZkPaths;
        this.oldCleanup = wrapTry(builder.oldCleanup);
        this.maxRandomSleepOnNotifyReload = builder.maxRandomSleepOnNotifyReload;
        this.broadcaster = builder.broadcaster;
        this.scheduleRunDuration = builder.scheduleRunDuration;
        this.executor = builder.executor;
        this.recycleListener = builder.recycleListener;
    }

    public static <T> ZkNotifyReloadCache<T> of(CacheFactory<T> cacheFactory, String notifyZkPath,
                                                Supplier<CuratorFramework> curatorFactory) {
        return ZkNotifyReloadCache.<T>newBuilder()
                .withCacheFactory(cacheFactory)
                .withNotifyZkPath(notifyZkPath)
                .withCuratorFactory(curatorFactory)
                .build();
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    @Override
    public T get() {
        if (cachedObject == null) {
            synchronized (ZkNotifyReloadCache.this) {
                if (cachedObject == null) {
                    cachedObject = init();
                }
            }
        }
        return cachedObject;
    }

    public Set<String> getZkNotifyPaths() {
        return notifyZkPaths;
    }

    @GuardedBy("this")
    private T init() {
        T obj;
        try {
            obj = cacheFactory.get(null);
        } catch (Throwable e) {
            if (firstAccessFailFactory != null) {
                obj = firstAccessFailFactory.get();
                logger.error("fail to build cache, using empty value:{}", obj, e);
            } else {
                throwIfUnchecked(e);
                throw new CacheBuildFailedException("fail to build cache.", e);
            }
        }

        if (obj != null) {
            if (postInitFuture == null) {
                // zk subscribe等操作放到另外的线程里执行，避免被 interrupt 之后，cache 构建整个失败
                // cache build本身的逻辑由使用方保证能处理好Thread interrupt
                SettableFuture<Void> future = SettableFuture.create();
                Thread t = new Thread(() -> {
                    try {
                        postCacheInit();
                        future.set(null);
                    } catch (Throwable e) {
                        future.setException(e);
                    }
                });
                t.setName("zkAutoReloadThread-postCacheInit-" + notifyZkPaths);
                t.setDaemon(true);
                t.start();
                postInitFuture = future;
            }

            try {
                postInitFuture.get();
            } catch (InterruptedException e) {
                // 被interrupt了也不能抛异常，直接设置interrupt标记，然cache构建就失败了
                // FixMe: Cache第一次注册zk，如果被打断了，就没有机会知道最终是注册成功还是注册失败了，后面的cache是可以直接返回值的
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (e.getCause() != null) {
                    // 正常情况应该都是走到这个分支，直接抛出原始异常
                    Throwables.throwIfUnchecked(e.getCause());
                }
                throw new CacheBuildFailedException("post cache init failed", e);
            }
        }
        return obj;
    }

    // zk注册，以及启动cache定时reload等逻辑
    private void postCacheInit() {
        if (broadcaster != null && notifyZkPaths != null) {
            notifyZkPaths.forEach(notifyZkPath -> {
                AtomicLong sleeping = new AtomicLong();
                AtomicLong lastNotifyTimestamp = new AtomicLong();
                broadcaster.subscribe(notifyZkPath, content -> {
                    long timestamp;
                    try {
                        timestamp = Long.parseLong(content);
                    } catch (Exception e) { // let error throw
                        logger.warn("parse notify timestamp {} failed", content, e);
                        timestamp = System.currentTimeMillis();
                    }
                    long lastNotify;
                    do {
                        lastNotify = lastNotifyTimestamp.get();
                        if (lastNotify == timestamp) {
                            logger.debug("notify with same timestamp {} with previous, skip", timestamp);
                            return;
                        }
                    } while (!lastNotifyTimestamp.compareAndSet(lastNotify, timestamp));

                    long deadline = sleeping.get();
                    if (deadline > 0L) {
                        logger.warn("ignore rebuild cache:{}, remaining sleep in:{}ms.",
                                notifyZkPath, (deadline - currentTimeMillis()));
                        return;
                    }
                    long sleepFor = ofNullable(maxRandomSleepOnNotifyReload)
                            .map(LongSupplier::getAsLong)
                            .filter(it -> it > 0)
                            .map(ThreadLocalRandom.current()::nextLong)
                            .orElse(0L);
                    sleeping.set(sleepFor + currentTimeMillis());
                    // executor should not be null when enable notify
                    executor.schedule(() -> {
                        sleeping.set(0L);
                        doRebuild();
                    }, sleepFor, MILLISECONDS);
                });
            });
        }
        if (scheduleRunDuration != null) {
            ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(1,
                    new ThreadFactoryBuilder()
                            .setPriority(MIN_PRIORITY)
                            .setNameFormat("zkAutoReloadThread-" + notifyZkPaths + "-%d")
                            .build());
            WeakReference<ZkNotifyReloadCache> cacheReference = new WeakReference<>(this);
            AtomicReference<Future<?>> futureReference = new AtomicReference<>();
            Runnable capturedRecycleListener = this.recycleListener;
            Future<?> scheduleFuture = scheduleWithDynamicDelay(scheduledExecutor, scheduleRunDuration, () -> {
                ZkNotifyReloadCache thisCache = cacheReference.get();

                if (thisCache == null) {
                    if (!scheduledExecutor.isShutdown()) {
                        if (futureReference.get() != null) {
                            // prevent from submitting next task
                            futureReference.get().cancel(true);
                        }
                        // ZkNotifyReloadCache has been recycled
                        scheduledExecutor.shutdownNow();
                        logger.warn("ZkNotifyReloadCache is recycled, path: {}", this.notifyZkPaths);
                        if (capturedRecycleListener != null) {
                            try {
                                capturedRecycleListener.run();
                            } catch (Throwable e) {
                                logger.error("run cache recycle listener error", e);
                            }
                        }
                    }
                    return;
                }
                thisCache.doRebuild();
            });
            futureReference.set(scheduleFuture);
        }
    }

    private void doRebuild() {
        synchronized (ZkNotifyReloadCache.this) {
            doRebuild0();
        }
    }

    private void doRebuild0() {
        T newObject = null;
        try {
            newObject = cacheFactory.get(cachedObject);
        } catch (Throwable e) {
            logger.error("fail to rebuild cache, remain the previous one.", e);
        }
        if (newObject != null) {
            T old = cachedObject;
            cachedObject = newObject;
            if (oldCleanup != null && old != cachedObject) {
                oldCleanup.accept(old);
            }
        }
    }

    @Override
    public void reload() {
        if (broadcaster != null && notifyZkPaths != null) {
            String content = String.valueOf(currentTimeMillis());
            notifyZkPaths.forEach(notifyZkPath -> broadcaster.broadcast(notifyZkPath,
                    content));
        } else {
            logger.warn("no zk broadcast or notify zk path found. ignore reload.");
        }
    }

    @Override
    public void reloadLocal() {
        synchronized (ZkNotifyReloadCache.this) {
            if (cachedObject != null) {
                doRebuild0();
            }
        }
    }

    private Supplier<T> wrapTry(CacheFactory<T> supplier) {
        if (supplier == null) {
            return null;
        }
        return () -> {
            try {
                return supplier.get();
            } catch (Throwable e) {
                logger.error("fail to create obj.", e);
                return null;
            }
        };
    }

    private Consumer<T> wrapTry(Consumer<T> consumer) {
        if (consumer == null) {
            return t -> {};
        }
        return t -> {
            try {
                consumer.accept(t);
            } catch (Throwable e) {
                logger.error("fail to cleanup.", e);
            }
        };
    }

    public static final class Builder<T> {

        private CacheFactoryEx<T> cacheFactory;
        private CacheFactory<T> firstAccessFailFactory;
        private Set<String> notifyZkPaths;
        private Consumer<T> oldCleanup;
        private LongSupplier maxRandomSleepOnNotifyReload;
        private Broadcaster broadcaster;
        private Supplier<Duration> scheduleRunDuration;
        @Nullable
        private ScheduledExecutorService executor;
        private Runnable recycleListener;

        @CheckReturnValue
        @Nonnull
        public Builder<T> subscribeThreadFactory(@Nonnull ThreadFactory threadFactory) {
            this.executor = newSingleThreadScheduledExecutor(checkNotNull(threadFactory));
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> enableAutoReload(@Nonnull Supplier<Duration> duration) {
            scheduleRunDuration = checkNotNull(duration);
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> enableAutoReload(long timeDuration, TimeUnit unit) {
            return enableAutoReload(() -> ofMillis(unit.toMillis(timeDuration)));
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withZkBroadcaster(ZkBroadcaster zkBroadcaster) {
            this.broadcaster = zkBroadcaster;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withBroadcaster(@Nonnull Broadcaster broadcaster) {
            this.broadcaster = requireNonNull(broadcaster);
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withCuratorFactory(Supplier<CuratorFramework> curatorFactory) {
            return withCuratorFactory(curatorFactory, null);
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withCuratorFactory(Supplier<CuratorFramework> curatorFactory,
                                             String broadcastPrefix) {
            this.broadcaster = new ZkBroadcaster(curatorFactory, broadcastPrefix);
            return this;
        }

        @Nonnull
        @CheckReturnValue
        public Builder<T> withCacheFactory(CacheFactory<T> cacheFactory) {
            this.cacheFactory = (prev) -> cacheFactory.get();
            return this;
        }

        @Nonnull
        @CheckReturnValue
        public Builder<T> withCacheFactoryEx(CacheFactoryEx<T> cacheFactoryEx) {
            this.cacheFactory = cacheFactoryEx;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> firstAccessFailObject(T obj) {
            if (obj != null) {
                this.firstAccessFailFactory = () -> obj;
            }
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> firstAccessFailFactory(CacheFactory<T> firstAccessFailFactory) {
            this.firstAccessFailFactory = firstAccessFailFactory;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withNotifyZkPath(String notifyZkPath) {
            if (notifyZkPaths == null) {
                notifyZkPaths = new HashSet<>();
            }
            this.notifyZkPaths.add(notifyZkPath);
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withOldCleanup(Consumer<T> oldCleanup) {
            this.oldCleanup = oldCleanup;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withMaxRandomSleepOnNotifyReload(long maxRandomSleepOnNotifyReloadInMs) {
            this.maxRandomSleepOnNotifyReload = () -> maxRandomSleepOnNotifyReloadInMs;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T>
        withMaxRandomSleepOnNotifyReload(LongSupplier maxRandomSleepOnNotifyReloadInMs) {
            this.maxRandomSleepOnNotifyReload = maxRandomSleepOnNotifyReloadInMs;
            return this;
        }

        @CheckReturnValue
        @Nonnull
        public Builder<T> withMaxRandomSleepOnNotifyReload(long maxRandomSleepOnNotify,
                                                           TimeUnit unit) {
            return withMaxRandomSleepOnNotifyReload(unit.toMillis(maxRandomSleepOnNotify));
        }

        /**
         * Set a listener which would be called when cached is gced and a resource release action is performed.
         */
        @CheckReturnValue
        @Nonnull
        public Builder<T> onResourceRecycled(Runnable recycleListener) {
            this.recycleListener = requireNonNull(recycleListener);
            return this;
        }

        @Nonnull
        public ZkNotifyReloadCache<T> build() {
            ensure();
            return new ZkNotifyReloadCache<>(this);
        }

        private void ensure() {
            checkNotNull(cacheFactory, "no cache factory.");
            if (notifyZkPaths != null && !notifyZkPaths.isEmpty()) {
                checkNotNull(broadcaster, "no broadcaster.");
                if (executor == null) {
                    executor = newSingleThreadScheduledExecutor();
                }
            }
        }
    }
}
