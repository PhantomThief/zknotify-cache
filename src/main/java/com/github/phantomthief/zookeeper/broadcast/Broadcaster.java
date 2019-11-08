package com.github.phantomthief.zookeeper.broadcast;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * A Interface to notify others, or receive notifies, at specified path.
 */
public interface Broadcaster {


    /**
     * keep for backward compatibility
     *
     * @deprecated using {@link #subscribe(String, Subscriber)}
     */
    @Deprecated
    default void subscribe(@Nonnull String path, @Nonnull Runnable subscriber) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(subscriber);
        subscribe(path, s -> subscriber.run());
    }

    /**
     * subscribe a path
     */
    void subscribe(@Nonnull String path, @Nonnull Subscriber subscriber);

    /**
     * notify Subscribers watched on the path, with content
     */
    void broadcast(String path, String content);

    /**
     * Interface for Broadcaster subscriber.
     */
    interface Subscriber {

        /**
         * Called when been notified.
         *
         * @param content the current content of subscribed path
         */
        void onChanged(String content);
    }
}
