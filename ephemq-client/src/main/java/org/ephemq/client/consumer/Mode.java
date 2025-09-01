package org.ephemq.client.consumer;

/**
 * The Mode enum represents the various modes of operation
 * that can be utilized for managing a message handling subscription.
 * <ul>
 * <li>REMAIN: Keeps the existing subscription as is.</li>
 * <li>APPEND: Adds new subscriptions to the existing ones.</li>
 * <li>DELETE: Removes existing subscriptions.</li>
 * </ul>
 */
public enum Mode {
    REMAIN, APPEND, DELETE
}