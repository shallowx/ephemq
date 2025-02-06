package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.ServerConfig;
import org.meteor.listener.LogListener;
import org.meteor.remote.exception.RemotingException;
import org.meteor.support.Manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Handles operations related to logs, including appending records, managing subscriptions,
 * and maintaining a log listener mechanism.
 */
public class LogHandler {
    /**
     * Holds the server configuration settings required by the LogHandler.
     * <p>
     * This variable stores an instance of ServerConfig, which provides
     * access to multiple configuration settings essential for server operations.
     */
    private final ServerConfig config;
    /**
     * The manager instance responsible for coordinating various subsystems
     * within the LogHandler.
     * <p>
     * This variable holds an implementation of the Manager interface, providing
     * methods to manage topics, clusters, connections, and executors, as well as
     * system-wide metrics and listeners.
     * <p>
     * It is initialized during the construction of the LogHandler and plays a
     * crucial role in the functionality provided by LogHandler, such as handling
     * logs, initializing configurations, managing connections, and more.
     */
    private final Manager manager;
    /**
     * A thread-safe map that maintains a ledger ID to Log mapping.
     * This map is used to store and retrieve Log instances based on their associated ledger ID.
     */
    private final Map<Integer, Log> ledgerIdOfLogs = new ConcurrentHashMap<>();
    /**
     * A list of LogListener objects that are registered to receive log events.
     * <p>
     * This list contains all the listeners that will be notified about various log-related events,
     * such as log initialization, message reception, synchronization, and message pushing.
     * The registered listeners must implement the LogListener interface.
     */
    private final ObjectList<LogListener> listeners = new ObjectArrayList<>();
    /**
     * Singleton executor service for scheduling storage cleanup tasks.
     * <p>
     * This executor is used to perform periodic cleanup operations on the storage system. It allows
     * for scheduled tasks to be executed in the background, ensuring that the storage system remains
     * efficient and free of unnecessary data without manual intervention.
     */
    private final ScheduledExecutorService cleanStorageScheduledExecutor;

    /**
     * Initializes a new instance of the LogHandler class, which is responsible for handling
     * log-related operations such as record appending, log maintenance, and cleaning.
     * This handler utilizes a scheduled thread pool to perform periodic storage cleaning tasks.
     *
     * @param config  the server configuration that provides various settings and parameters required
     *                for initializing the log handler.
     * @param manager the manager instance that facilitates interactions and coordination between
     *                different components within the system, including topic handling, cluster management,
     *                connection management, and event handling.
     */
    public LogHandler(ServerConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.cleanStorageScheduledExecutor =
                Executors.newScheduledThreadPool(1, Thread.ofVirtual().name("storage-cleaner").factory());
    }

    /**
     * Starts the scheduled task to clean storage for all logs in the system.
     * <p>
     * This method schedules a fixed-rate task that iterates through all
     * the logs in the `ledgerIdOfLogs` map and invokes their `cleanStorage`
     * method every 30 seconds, with an initial delay of 5 seconds.
     */
    public void start() {
        this.cleanStorageScheduledExecutor.scheduleAtFixedRate(() -> {
            for (Log log : ledgerIdOfLogs.values()) {
                log.cleanStorage();
            }
        }, 30, 5, TimeUnit.SECONDS);
    }

    /**
     * Appends a record to the specified ledger with the given marker and payload.
     *
     * @param ledger the ledger ID where the record will be appended
     * @param marker the marker associated with the record
     * @param payload the content of the record to be appended
     * @param promise the promise to be fulfilled with the offset of the appended record or a failure
     */
    public void appendRecord(int ledger, int marker, ByteBuf payload, Promise<Offset> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                    String.format("Ledger[%d] ot found", ledger)));
            return;
        }

        for (LogListener listener : listeners) {
            TopicPartition topicPartition = log.getTopicPartition();
            listener.onReceiveMessage(topicPartition.topic(), ledger, 1);
        }
        log.append(marker, payload, promise);
    }

    /**
     * Alters the log subscription for a specified ledger.
     *
     * @param channel The communication channel associated with the subscription.
     * @param ledger The identifier of the ledger to alter the subscription for.
     * @param addMarkers Collection of markers to add to the subscription.
     * @param deleteMarkers Collection of markers to remove from the subscription.
     * @param promise The promise to be fulfilled with the result of the operation.
     */
    public void alterSubscribe(Channel channel, int ledger, IntCollection addMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(
                    RemotingException.Failure.PROCESS_EXCEPTION, STR."Ledger[\{ledger}] not found"));
            return;
        }

        log.alterSubscribe(channel, addMarkers, deleteMarkers, promise);
    }

    /**
     * Cleans the subscription for the specified ledger and channel.
     *
     * @param channel The channel associated with the subscription.
     * @param ledger The ledger ID for which the subscription needs to be cleaned.
     * @param promise A promise that will be fulfilled with the result of the clean up operation.
     */
    public void cleanSubscribe(Channel channel, int ledger, Promise<Boolean> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.trySuccess(null);
            return;
        }
        log.cleanSubscribe(channel, promise);
    }

    /**
     * Resets the subscription for a specific ledger, identified by its epoch and index, and sets the provided markers.
     *
     * @param ledger the ID of the ledger for which the subscription is to be reset
     * @param epoch the epoch of the subscription point
     * @param index the index within the epoch for the subscription point
     * @param channel the channel associated with the subscription
     * @param markers the collection of markers to be set upon resetting the subscription
     * @param promise the promise to be fulfilled with the result of the reset operation
     */
    public void resetSubscribe(int ledger, int epoch, long index, Channel channel, IntCollection markers, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(
                    RemotingException.Failure.PROCESS_EXCEPTION, STR."Ledger[\{ledger}] not found"));
            return;
        }

        log.resetSubscribe(channel, Offset.of(epoch, index), markers, promise);
    }

    /**
     * Adds a list of LogListener instances to the current list of listeners.
     *
     * @param logListeners A list of LogListener instances to be added to the existing listeners.
     */
    public void addLogListener(List<LogListener> logListeners) {
        listeners.addAll(logListeners);
    }

    /**
     * Initializes a log for the given topic partition, ledger ID, epoch, and topic configuration.
     *
     * @param topicPartition the topic partition for the log
     * @param ledgerId the ledger ID for the log
     * @param epoch the epoch for the log
     * @param topicConfig the configuration for the topic
     * @return the initialized log
     */
    public Log initLog(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) {
        Log log = new Log(config, topicPartition, ledgerId, epoch, manager, topicConfig);
        this.ledgerIdOfLogs.putIfAbsent(ledgerId, log);
        for (LogListener listener : listeners) {
            listener.onInitLog(log);
        }
        return log;
    }

    /**
     * Retrieves the Log object associated with the specified ledger id.
     *
     * @param ledger the identifier for the ledger whose Log is to be retrieved
     * @return the Log associated with the specified ledger id
     */
    public Log getLog(int ledger) {
        return ledgerIdOfLogs.get(ledger);
    }

    /**
     * Gets the log associated with the given ledger ID, or initializes it using the provided function if it does not exist.
     *
     * @param ledger the ID of the ledger to get or initialize the log for
     * @param f the function to compute the log if it is not already present
     * @return the existing or newly initialized log for the given ledger ID
     */
    public Log getOrInitLog(int ledger, Function<Integer, Log> f) {
        return this.ledgerIdOfLogs.computeIfAbsent(ledger, f);
    }

    /**
     * Retrieves the mapping of ledger IDs to log instances.
     *
     * @return a map where the keys are ledger IDs and the values are corresponding log instances.
     */
    public Map<Integer, Log> getLedgerIdOfLogs() {
        return ledgerIdOfLogs;
    }

    /**
     * Shuts down the log handler by destroying all logs associated with their respective ledger IDs.
     * This method iterates over the ledgerIdOfLogs map and calls the destroyLog method for each ledger ID key.
     */
    public void shutdown() {
        for (Integer ledgerId : this.ledgerIdOfLogs.keySet()) {
            destroyLog(ledgerId);
        }
    }

    /**
     * Destroys the log associated with the specified ledger ID.
     * If a log corresponding to the given ledger ID exists, it will be closed.
     *
     * @param ledgerId the ID of the ledger for which the log will be destroyed
     */
    public void destroyLog(int ledgerId) {
        Log log = this.ledgerIdOfLogs.get(ledgerId);
        if (log != null) {
            log.close(null);
        }
    }

    /**
     * Checks if a log identified by the specified ledger ID exists in the log handler.
     *
     * @param ledgerId The ID of the ledger to check.
     * @return true if the ledger ID exists in the log handler, false otherwise.
     */
    public boolean contains(int ledgerId) {
        return ledgerIdOfLogs.containsKey(ledgerId);
    }

    /**
     * Retrieves the list of log listeners.
     *
     * @return a list of LogListener objects currently registered.
     */
    public List<LogListener> getLogListeners() {
        return listeners;
    }

    /**
     * Save synchronization data for the specified ledger.
     *
     * @param channel the channel through which the data is being transmitted
     * @param ledger the identifier of the ledger to which data belongs
     * @param count the number of records to be synchronized
     * @param data the data to be synchronized
     * @param promise the promise to be completed with the result of the operation
     */
    public void saveSyncData(Channel channel, int ledger, int count, ByteBuf data, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                    STR."Ledger[\{ledger}] not found"));
            return;
        }

        for (LogListener listener : listeners) {
            TopicPartition topicPartition = log.getTopicPartition();
            listener.onSyncMessage(topicPartition.topic(), ledger, count);
        }
        log.appendChunk(channel, count, data, promise);
    }
}
