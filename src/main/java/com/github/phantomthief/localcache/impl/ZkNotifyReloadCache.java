/**
 * 
 */
package com.github.phantomthief.localcache.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;

import com.github.phantomthief.localcache.CacheFactory;
import com.github.phantomthief.localcache.ReloadableCache;
import com.github.phantomthief.zookeeper.broadcast.ZkBroadcaster;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class ZkNotifyReloadCache<T> implements ReloadableCache<T> {

    private static Logger logger = getLogger(ZkNotifyReloadCache.class);

    private final Supplier<T> cacheFactory;
    private final Supplier<T> firstAccessFailFactory;
    private final Set<String> notifyZkPaths;
    private final Consumer<T> oldCleanup;
    private final long maxRandomSleepOnNotifyReload;
    private final ZkBroadcaster zkBroadcaster;
    private final long scheduleRunDuration;

    private volatile T cachedObject;

    private ZkNotifyReloadCache(CacheFactory<T> cacheFactory,
            CacheFactory<T> firstAccessFailFactory, Set<String> notifyZkPaths,
            Consumer<T> oldCleanup, long maxRandomSleepOnNotifyReload, ZkBroadcaster zkBroadcaster,
            long scheduleRunDuration) {
        this.cacheFactory = wrapTry(cacheFactory);
        this.firstAccessFailFactory = wrapTry(firstAccessFailFactory);
        this.notifyZkPaths = notifyZkPaths;
        this.oldCleanup = wrapTry(oldCleanup);
        this.maxRandomSleepOnNotifyReload = maxRandomSleepOnNotifyReload;
        this.zkBroadcaster = zkBroadcaster;
        this.scheduleRunDuration = scheduleRunDuration;
    }

    public static <T> ZkNotifyReloadCache<T> of(CacheFactory<T> cacheFactory, String notifyZkPath,
            Supplier<CuratorFramework> curatorFactory) {
        return ZkNotifyReloadCache.<T> newBuilder() //
                .withCacheFactory(cacheFactory) //
                .withNotifyZkPath(notifyZkPath) //
                .withCuratorFactory(curatorFactory) //
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

    private T init() {
        T obj = cacheFactory.get();
        if (obj == null && firstAccessFailFactory != null) {
            obj = firstAccessFailFactory.get();
        }
        if (obj != null) {
            if (zkBroadcaster != null && notifyZkPaths != null) {
                notifyZkPaths.forEach(notifyZkPath -> zkBroadcaster.subscribe(notifyZkPath, () -> {
                    if (maxRandomSleepOnNotifyReload > 0) {
                        sleepUninterruptibly(
                                ThreadLocalRandom.current().nextLong(maxRandomSleepOnNotifyReload),
                                MILLISECONDS);
                    }
                    synchronized (ZkNotifyReloadCache.this) {
                        T newObject = cacheFactory.get();
                        if (newObject != null) {
                            T old = cachedObject;
                            cachedObject = newObject;
                            if (oldCleanup != null && old != cachedObject) {
                                oldCleanup.accept(old);
                            }
                        }
                    }
                }));
            }
            if (scheduleRunDuration > 0) {
                ScheduledExecutorService scheduledExecutorService = newScheduledThreadPool(1,
                        new ThreadFactoryBuilder() //
                                .setPriority(Thread.MIN_PRIORITY) //
                                .setNameFormat("zkAutoReloadThread-" + notifyZkPaths + "-%d") //
                                .build());
                scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    synchronized (ZkNotifyReloadCache.this) {
                        T newObject = cacheFactory.get();
                        if (newObject != null) {
                            T old = cachedObject;
                            cachedObject = newObject;
                            if (oldCleanup != null || old != cachedObject) {
                                oldCleanup.accept(old);
                            }
                        }
                    }
                }, scheduleRunDuration, scheduleRunDuration, MILLISECONDS);
            }
        }
        return obj;
    }

    @Override
    public void reload() {
        if (zkBroadcaster != null && notifyZkPaths != null) {
            notifyZkPaths.forEach(notifyZkPath -> zkBroadcaster.broadcast(notifyZkPath,
                    String.valueOf(System.currentTimeMillis())));
        } else {
            logger.warn("no zk broadcast or notify zk path found. ignore reload.");
        }
    }

    @Override
    public void reloadLocal() {
        synchronized (ZkNotifyReloadCache.this) {
            if (cachedObject != null) {
                T newObject = cacheFactory.get();
                if (newObject != null) {
                    T old = cachedObject;
                    cachedObject = newObject;
                    if (oldCleanup != null && old != cachedObject) {
                        oldCleanup.accept(old);
                    }
                }
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

        private CacheFactory<T> cacheFactory;
        private CacheFactory<T> firstAccessFailFactory;
        private Set<String> notifyZkPaths;
        private Consumer<T> oldCleanup;
        private long maxRandomSleepOnNotifyReload;
        private ZkBroadcaster zkBroadcaster;
        private long scheduleRunDruation;

        public Builder<T> enableAutoReload(long timeDuration, TimeUnit unit) {
            scheduleRunDruation = unit.toMillis(timeDuration);
            return this;
        }

        public Builder<T> withZkBroadcaster(ZkBroadcaster zkBroadcaster) {
            this.zkBroadcaster = zkBroadcaster;
            return this;
        }

        public Builder<T> withCuratorFactory(Supplier<CuratorFramework> curatorFactory) {
            return withCuratorFactory(curatorFactory, null);
        }

        public Builder<T> withCuratorFactory(Supplier<CuratorFramework> curatorFactory,
                String broadcastPrefix) {
            this.zkBroadcaster = new ZkBroadcaster(curatorFactory, broadcastPrefix);
            return this;
        }

        public Builder<T> withCacheFactory(CacheFactory<T> cacheFactory) {
            this.cacheFactory = cacheFactory;
            return this;
        }

        public Builder<T> firstAccessFailObject(T obj) {
            if (obj != null) {
                this.firstAccessFailFactory = () -> obj;
            }
            return this;
        }

        public Builder<T> firstAccessFailFactory(CacheFactory<T> firstAccessFailFactory) {
            this.firstAccessFailFactory = firstAccessFailFactory;
            return this;
        }

        public Builder<T> withNotifyZkPath(String notifyZkPath) {
            if (notifyZkPaths == null) {
                notifyZkPaths = new HashSet<>();
            }
            this.notifyZkPaths.add(notifyZkPath);
            return this;
        }

        public Builder<T> withOldCleanup(Consumer<T> oldCleanup) {
            this.oldCleanup = oldCleanup;
            return this;
        }

        public Builder<T> withMaxRandomSleepOnNotifyReload(long maxRandomSleepOnNotifyReloadInMs) {
            this.maxRandomSleepOnNotifyReload = maxRandomSleepOnNotifyReloadInMs;
            return this;
        }

        public Builder<T> withMaxRandomSleepOnNotifyReload(long maxRandomSleepOnNotify,
                TimeUnit unit) {
            return withMaxRandomSleepOnNotifyReload(unit.toMillis(maxRandomSleepOnNotify));
        }

        public ZkNotifyReloadCache<T> build() {
            ensure();
            return new ZkNotifyReloadCache<>(cacheFactory, firstAccessFailFactory, notifyZkPaths,
                    oldCleanup, maxRandomSleepOnNotifyReload, zkBroadcaster, scheduleRunDruation);
        }

        private void ensure() {
            checkNotNull(cacheFactory, "no cache factory.");
            if (notifyZkPaths != null && !notifyZkPaths.isEmpty()) {
                checkNotNull(zkBroadcaster, "no zk broadcaster.");
            }
        }
    }
}
