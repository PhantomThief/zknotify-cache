package com.github.phantomthief.localcache.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.MIN_PRIORITY;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

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
    private final ScheduledExecutorService executor;

    private volatile T cachedObject;

    private ZkNotifyReloadCache(Builder<T> builder) {
        this.cacheFactory = wrapTry(builder.cacheFactory);
        this.firstAccessFailFactory = wrapTry(builder.firstAccessFailFactory);
        this.notifyZkPaths = builder.notifyZkPaths;
        this.oldCleanup = wrapTry(builder.oldCleanup);
        this.maxRandomSleepOnNotifyReload = builder.maxRandomSleepOnNotifyReload;
        this.zkBroadcaster = builder.zkBroadcaster;
        this.scheduleRunDuration = builder.scheduleRunDuration;
        this.executor = builder.executor;
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
                notifyZkPaths.forEach(notifyZkPath -> {
                    AtomicLong sleeping = new AtomicLong();
                    zkBroadcaster.subscribe(notifyZkPath, () -> { //
                        long deadline = sleeping.get();
                        if (deadline > 0L) {
                            logger.warn("ignore rebuild cache:{}, remaining sleep in:{}ms.",
                                    notifyZkPath, (deadline - currentTimeMillis()));
                            return;
                        }
                        long sleepFor = current().nextLong(maxRandomSleepOnNotifyReload);
                        sleeping.set(sleepFor + currentTimeMillis());
                        executor.schedule(() -> {
                            sleeping.set(0L);
                            doRebuild();
                        }, sleepFor, MILLISECONDS);
                    });
                });
            }
            if (scheduleRunDuration > 0) {
                ScheduledExecutorService scheduledExecutorService = newScheduledThreadPool(1,
                        new ThreadFactoryBuilder() //
                                .setPriority(MIN_PRIORITY) //
                                .setNameFormat("zkAutoReloadThread-" + notifyZkPaths + "-%d") //
                                .build());
                scheduledExecutorService.scheduleWithFixedDelay(this::doRebuild,
                        scheduleRunDuration, scheduleRunDuration, MILLISECONDS);
            }
        }
        return obj;
    }

    private void doRebuild() {
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
    }

    @Override
    public void reload() {
        if (zkBroadcaster != null && notifyZkPaths != null) {
            notifyZkPaths.forEach(notifyZkPath -> zkBroadcaster.broadcast(notifyZkPath,
                    String.valueOf(currentTimeMillis())));
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
        private long scheduleRunDuration;
        private ScheduledExecutorService executor;

        public Builder<T> subscribeThreadFactory(@Nonnull ThreadFactory threadFactory) {
            this.executor = newSingleThreadScheduledExecutor(checkNotNull(threadFactory));
            return this;
        }

        public Builder<T> enableAutoReload(long timeDuration, TimeUnit unit) {
            scheduleRunDuration = unit.toMillis(timeDuration);
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
            return new ZkNotifyReloadCache<>(this);
        }

        private void ensure() {
            checkNotNull(cacheFactory, "no cache factory.");
            if (notifyZkPaths != null && !notifyZkPaths.isEmpty()) {
                checkNotNull(zkBroadcaster, "no zk broadcaster.");
            }
            if (executor == null) {
                executor = newSingleThreadScheduledExecutor();
            }
        }
    }
}
