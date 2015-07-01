/**
 * 
 */
package com.github.phantomthief.zookeeper.broadcast;

/**
 * @author w.vela
 */
public interface ZkSubscriber {

    public void handle(byte[] content);
}
