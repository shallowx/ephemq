package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.ServerConfig;
import org.meteor.coordinatior.Coordinator;
import org.meteor.listener.LogListener;
import org.meteor.remote.processor.RemoteException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class LogCoordinator {
    private final ServerConfig config;
    private final Coordinator coordinator;
    private final Map<Integer, Log> ledgerId2LogMap = new ConcurrentHashMap<>();
    private final List<LogListener> listeners = new LinkedList<>();
    private final ScheduledExecutorService scheduledExecutorService;

    public LogCoordinator(ServerConfig config, Coordinator coordinator) {
        this.config = config;
        this.coordinator = coordinator;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("storage-cleaner"));
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            for (Log log : ledgerId2LogMap.values()) {
                log.cleanStorage();
            }
        }, 30, 5, TimeUnit.SECONDS);
    }

    public void appendRecord(int ledger, int marker, ByteBuf payload, Promise<Offset> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d ot found", ledger)));
            return;
        }

        for (LogListener listener : listeners) {
            TopicPartition topicPartition = log.getTopicPartition();
            listener.onReceiveMessage(topicPartition.getTopic(), ledger, 1);
        }
        log.append(marker, payload, promise);
    }

    public void alterSubscribe(Channel channel, int ledger, IntCollection addMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d ot found", ledger)));
            return;
        }

        log.alterSubscribe(channel, addMarkers, deleteMarkers, promise);
    }

    public void cleanSubscribe(Channel channel, int ledger, Promise<Boolean> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.trySuccess(null);
            return;
        }

        log.cleanSubscribe(channel, promise);
    }

    public void resetSubscribe(int ledger, int epoch, long index, Channel channel, IntCollection markers, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d ot found", ledger)));
            return;
        }

        log.resetSubscribe(channel, Offset.of(epoch, index), markers, promise);
    }

    public void addLogListener(List<LogListener> logListeners) {
        listeners.addAll(logListeners);
    }

    public Log initLog(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) {
        Log log = new Log(config, topicPartition, ledgerId, epoch, coordinator, topicConfig);
        this.ledgerId2LogMap.putIfAbsent(ledgerId, log);
        for (LogListener listener : listeners) {
            listener.onInitLog(log);
        }
        return log;
    }

    public Log getLog(int ledger) {
        return ledgerId2LogMap.get(ledger);
    }

    public Log getOrInitLog(int ledger, Function<Integer, Log> f) {
        return this.ledgerId2LogMap.computeIfAbsent(ledger, f);
    }

    public Map<Integer, Log> getLedgerId2LogMap() {
        return ledgerId2LogMap;
    }

    public void shutdown() {
        for (Integer ledgerId : this.ledgerId2LogMap.keySet()) {
            destroyLog(ledgerId);
        }
    }

    public void destroyLog(int ledgerId) {
        Log log = this.ledgerId2LogMap.get(ledgerId);
        if (log != null) {
            log.close(null);
        }
    }

    public boolean contains(int ledgerId) {
        return ledgerId2LogMap.containsKey(ledgerId);
    }

    public List<LogListener> getLogListeners() {
        return listeners;
    }

    public void saveSyncData(Channel channel, int ledger, int count, ByteBuf data, Promise<Integer> promise) {
        Log log = getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d not found", ledger)));
            return;
        }

        for (LogListener listener : listeners) {
            TopicPartition topicPartition = log.getTopicPartition();
            listener.onSyncMessage(topicPartition.getTopic(), ledger, count);
        }
        log.appendChunk(channel, count, data, promise);
    }
}
