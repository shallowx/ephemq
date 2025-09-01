package org.ephemq.client.consumer;

import java.util.Map;

/**
 * An interface for consumer functionalities in a messaging system. This interface defines methods for starting
 * the consumer, subscribing to topics and queues, canceling subscriptions, clearing subscriptions on a specific
 * topic, and closing the consumer.
 */
public interface Consumer {
    /**
     * Starts the consumer to begin processing messages from the subscribed topics and queues.
     * <p>
     * This method must be called before any message processing can take place. It sets up the necessary
     * resources and configurations for the consumer to start receiving messages. Subsequent subscriptions
     * to topics or queues will be active only after the consumer has been started.
     * <p>
     * The start method should be invoked after configuring the consumer with client and consumer settings,
     * such as bootstrap addresses, connection pool capacity, and other related configurations.
     */
    void start();

    /**
     * Subscribes to a specified topic and queue. This method registers the consumer to listen
     * for messages from the given topic and queue within the messaging system.
     *
     * @param topic the topic to which the consumer subscribes
     * @param queue the queue associated with the topic
     */
    void subscribe(String topic, String queue);

    /**
     * Subscribes the consumer to specified message queues of various topics.
     * This method takes a map where the keys are topic names and the values
     * are the corresponding queue names. It sets up subscriptions for each
     * topic-queue pair provided in the map.
     *
     * @param ships a map where keys are topic names and values are queue names
     */
    void subscribe(Map<String, String> ships);

    /**
     * Cancels the subscription to a specified queue under the given topic.
     *
     * @param topic the topic from which the subscription is to be canceled
     * @param queue the specific queue under the topic whose subscription is to be canceled
     */
    void cancelSubscribe(String topic, String queue);

    /**
     * Clears all subscriptions to the specified topic.
     *
     * @param topic the topic for which to clear subscriptions
     */
    void clear(String topic);

    /**
     * Closes the consumer, effectively stopping any ongoing message consumption and releasing resources.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for the consumer to shut down
     */
    void close() throws InterruptedException;
}
