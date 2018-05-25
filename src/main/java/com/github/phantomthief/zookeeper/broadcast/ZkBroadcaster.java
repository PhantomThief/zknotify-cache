package com.github.phantomthief.zookeeper.broadcast;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.apache.curator.utils.ZKPaths.makePath;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

/**
 * @author w.vela
 */
public class ZkBroadcaster {

    private static final String DEFAULT_ZK_PREFIX = "/broadcast";

    private final Logger logger = getLogger(getClass());

    private final Supplier<CuratorFramework> curatorFactory;
    private final String zkPrefix;
    private final ConcurrentMap<String, Set<Runnable>> subscribeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeCache> nodeCacheMap = new ConcurrentHashMap<>();

    public ZkBroadcaster(Supplier<CuratorFramework> curatorFactory, String zkPrefix) {
        this.curatorFactory = curatorFactory;
        this.zkPrefix = zkPrefix;
    }

    public ZkBroadcaster(Supplier<CuratorFramework> curatorFactory) {
        this(curatorFactory, DEFAULT_ZK_PREFIX);
    }

    public void subscribe(@Nonnull String path, @Nonnull Runnable subscriber) {
        checkNotNull(path);
        checkNotNull(subscriber);

        Set<Runnable> subscribers = subscribeMap.compute(path, (k, oldSet) -> {
            if (oldSet == null) {
                oldSet = new HashSet<>();
            }
            oldSet.add(subscriber);
            return oldSet;
        });

        nodeCacheMap.computeIfAbsent(path, p -> {
            CuratorFramework curatorFramework = curatorFactory.get();
            NodeCache nodeCache = new NodeCache(curatorFramework, makePath(zkPrefix, p));
            try {
                nodeCache.start();
                nodeCache.rebuild();
            } catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            nodeCache.getListenable().addListener(() -> subscribers.parallelStream().forEach(s -> { //
                try {
                    s.run();
                } catch (Throwable e) {
                    logger.error("Ops. fail to do handle for:{}->{}", zkPrefix, s, e);
                }
            }));
            return nodeCache;
        });
    }

    public void broadcast(String path, String content) {
        String realPath = makePath(zkPrefix, path);
        try {
            try {
                curatorFactory.get().setData().forPath(realPath, content.getBytes());
            } catch (KeeperException.NoNodeException e) {
                curatorFactory.get().create().creatingParentsIfNeeded().forPath(realPath,
                        content.getBytes());
            }
        } catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
