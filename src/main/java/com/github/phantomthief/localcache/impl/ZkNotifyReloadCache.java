/**
 * 
 */
package com.github.phantomthief.localcache.impl;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;

import com.github.phantomthief.localcache.CacheFactory;
import com.github.phantomthief.localcache.ReloadableCache;
import com.github.phantomthief.zookeeper.broadcast.ZkBroadcaster;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class ZkNotifyReloadCache<T> implements ReloadableCache<T> {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final Supplier<T> cacheFactory;
    private final Supplier<T> firstAccessFailFactory;
    private final String notifyZkPath;
    private final Consumer<T> oldCleanup;
    private final int maxRandomSleepOnNotifyReload;
    private final Random random;
    private final ZkBroadcaster zkBroadcaster;
    private final long scheduleRunDruation;

    private volatile T cachedObject;

    private ZkNotifyReloadCache(CacheFactory<T> cacheFactory,
            CacheFactory<T> firstAccessFailFactory, String notifyZkPath, Consumer<T> oldCleanup,
            int maxRandomSleepOnNotifyReload, ZkBroadcaster zkBroadcaster,
            long scheduleRunDruation) {
        this.cacheFactory = wrapTry(cacheFactory);
        this.firstAccessFailFactory = wrapTry(firstAccessFailFactory);
        this.notifyZkPath = notifyZkPath;
        this.oldCleanup = wrapTry(oldCleanup);
        this.maxRandomSleepOnNotifyReload = maxRandomSleepOnNotifyReload;
        this.random = maxRandomSleepOnNotifyReload > 0 ? new Random() : null;
        this.zkBroadcaster = zkBroadcaster;
        this.scheduleRunDruation = scheduleRunDruation;
    }

    /* (non-Javadoc)
     * @see java.util.function.Supplier#get()
     */
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

    private T init() {
        T obj = cacheFactory.get();
        if (obj == null && firstAccessFailFactory != null) {
            obj = firstAccessFailFactory.get();
        }
        if (obj != null) {
            zkBroadcaster.subscribe(notifyZkPath, c -> {
                if (maxRandomSleepOnNotifyReload > 0) {
                    try {
                        Thread.sleep(random.nextInt(maxRandomSleepOnNotifyReload));
                    } catch (Exception idontcare) {
                        // i don't care.
                    }
                }
                synchronized (ZkNotifyReloadCache.this) {
                    T newObject = cacheFactory.get();
                    if (newObject != null) {
                        T old = cachedObject;
                        cachedObject = newObject;
                        if (oldCleanup != null) {
                            oldCleanup.accept(old);
                        }
                    }
                }
            });
            if (scheduleRunDruation > 0) {
                ScheduledExecutorService scheduledExecutorService = Executors
                        .newScheduledThreadPool(1,
                                new ThreadFactoryBuilder() //
                                        .setPriority(Thread.MIN_PRIORITY) //
                                        .setNameFormat("zkAutoReloadThread-" + notifyZkPath + "-%d") //
                                        .build());
                scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    synchronized (ZkNotifyReloadCache.this) {
                        T newObject = cacheFactory.get();
                        if (newObject != null) {
                            T old = cachedObject;
                            cachedObject = newObject;
                            if (oldCleanup != null) {
                                oldCleanup.accept(old);
                            }
                        }
                    }
                } , scheduleRunDruation, scheduleRunDruation, TimeUnit.MILLISECONDS);
            }
        }
        return obj;
    }

    /* (non-Javadoc)
     * @see com.kuaishou.framework.localcache.n.ReloadableCache#reload()
     */
    @Override
    public void reload() {
        zkBroadcaster.broadcast(notifyZkPath, String.valueOf(System.currentTimeMillis()));
    }

    private Supplier<T> wrapTry(CacheFactory<T> supplier) {
        if (supplier == null) {
            return null;
        }
        return () -> {
            try {
                return supplier.get();
            } catch (Throwable e) {
                logger.error("fail to create obj.", e);;
                return null;
            }
        };
    }

    private Consumer<T> wrapTry(Consumer<T> consumer) {
        if (consumer == null) {
            return null;
        }
        return t -> {
            try {
                consumer.accept(t);
            } catch (Throwable e) {
                logger.error("fail to cleanup.", e);;
            }
        };
    }

    public static final <T> ZkNotifyReloadCache<T> of(CacheFactory<T> cacheFactory,
            String notifyZkPath, Supplier<CuratorFramework> curatorFactory) {
        return ZkNotifyReloadCache.<T> newBuilder() //
                .withCacheFactory(cacheFactory) //
                .withNotifyZkPath(notifyZkPath) //
                .withCuratorFactory(curatorFactory) //
                .build();
    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {

        private CacheFactory<T> cacheFactory;
        private CacheFactory<T> firstAccessFailFactory;
        private String notifyZkPath;
        private Consumer<T> oldCleanup;
        private int maxRandomSleepOnNotifyReload;
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
            this.notifyZkPath = notifyZkPath;
            return this;
        }

        public Builder<T> withOldCleanup(Consumer<T> oldCleanup) {
            this.oldCleanup = oldCleanup;
            return this;
        }

        public Builder<T> withMaxRandomSleepOnNotifyReload(int maxRandomSleepOnNotifyReload) {
            this.maxRandomSleepOnNotifyReload = maxRandomSleepOnNotifyReload;
            return this;
        }

        public ZkNotifyReloadCache<T> build() {
            ensure();
            return new ZkNotifyReloadCache<>(cacheFactory, firstAccessFailFactory, notifyZkPath,
                    oldCleanup, maxRandomSleepOnNotifyReload, zkBroadcaster, scheduleRunDruation);
        }

        private void ensure() {
            if (cacheFactory == null) {
                throw new IllegalArgumentException("no cache factory.");
            }
            if (notifyZkPath == null) {
                throw new IllegalArgumentException("no notify zk path.");
            }
            if (zkBroadcaster == null) {
                throw new IllegalArgumentException("no zk broadcaster.");
            }
        }

    }

}
