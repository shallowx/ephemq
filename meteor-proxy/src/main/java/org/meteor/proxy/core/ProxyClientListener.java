package org.meteor.proxy.core;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.core.*;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.Log;
import org.meteor.proxy.support.LedgerSyncSupport;
import org.meteor.proxy.support.MeterProxyException;
import org.meteor.proxy.support.ProxyTopicHandleSupport;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;
import org.meteor.support.Manager;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.meteor.metrics.config.MetricsConstants.*;

/**
 * ProxyClientListener is a listener class that handles events related to proxy clients.
 * It implements the CombineListener interface to manage synchronization messages, channel closures,
 * topic changes, and node offline signals.
 */
public class ProxyClientListener implements CombineListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyClientListener.class);
    /**
     * A map that holds distribution summaries for counts, keyed by integer identifiers.
     * This map is thread-safe and allows concurrent access and modifications, utilizing
     * a ConcurrentHashMap to ensure proper synchronization.
     */
    protected final Map<Integer, DistributionSummary> countSummaries = new ConcurrentHashMap<>();
    /**
     * The manager instance responsible for handling various server components and their interactions within the ProxyClientListener.
     * It is used to start up and shut down operations, and provides access to supporting components such as topic handles,
     * cluster management, logging, connections, and event executors.
     */
    private final Manager manager;
    /**
     * Handles synchronization support for ledger operations.
     * <p>
     * This object is used to facilitate ledger synchronization tasks such as
     * syncing ledgers from upstream servers and managing associated processes.
     */
    private final LedgerSyncSupport syncSupport;
    /**
     * Holds the configuration settings required for setting up and managing the proxy.
     * It includes details such as upstream server information, thread limits,
     * synchronization intervals, and timeout settings necessary for the proxy's operation.
     */
    private final ProxyConfig proxyConfiguration;
    /**
     * Represents the client connected to the proxy.
     * This client is used to communicate with the proxy server
     * and handle various signals and messages during synchronization.
     */
    private Client client;
    /**
     * A thread-local variable that holds a {@link Semaphore} instance for controlling access to
     * proxy leader synchronization operations. Each thread accessing this variable will get its
     * own {@link Semaphore} initialized with the value retrieved from the proxy configuration.
     */
    private final FastThreadLocal<Semaphore> threadSemaphore = new FastThreadLocal<>() {
        @Override
        protected Semaphore initialValue() {
            return new Semaphore(proxyConfiguration.getProxyLeaderSyncSemaphore());
        }
    };

    /**
     * Constructs a ProxyClientListener with the given configuration, manager, and sync support.
     *
     * @param proxyConfiguration the ProxyConfig object holding configuration properties for the proxy client.
     * @param manager the Manager instance responsible for managing various server components and their interactions.
     * @param syncSupport the LedgerSyncSupport instance providing support for ledger synchronization operations.
     */
    public ProxyClientListener(ProxyConfig proxyConfiguration, Manager manager,
                               LedgerSyncSupport syncSupport) {
        this.proxyConfiguration = proxyConfiguration;
        this.manager = manager;
        this.syncSupport = syncSupport;
        EventExecutor taskExecutor = manager.getAuxEventExecutorGroup().next();
        taskExecutor.scheduleWithFixedDelay(this::checkSync, proxyConfiguration.getProxySyncCheckIntervalMilliseconds(), proxyConfiguration.getProxySyncCheckIntervalMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Checks the synchronization state of logs for various topics and ledgers.
     * Iterates through all logs managed by the log handler and verifies if
     * synchronization channels and message routers are properly configured
     * and active. If any discrepancies are found, it attempts to resume
     * synchronization through a designated executor.
     * <p>
     * This method performs the following steps:
     * 1. Retrieves the map of logs from the log handler.
     * 2. Iterates through each log and checks for the presence and validity
     *    of sync channels and message routers.
     * 3. If the sync channel or message router is missing or invalid, logs
     *    a debug message and continues to the next log.
     * 4. If the message router and sync channel are valid, retrieves the
     *    replicas participating in the ledger.
     * 5. If the ledger participants are valid and the sync channel is
     *    inactive or disconnected, attempts to resume synchronization
     *    through an executor.
     */
    private void checkSync() {
        Map<Integer, Log> map = manager.getLogHandler().getLedgerIdOfLogs();
        if (map == null) {
            return;
        }
        Collection<Log> logs = map.values();
        for (Log log : logs) {
            String topic = log.getTopic();
            int ledger = log.getLedger();
            ClientChannel syncChannel = log.getSyncChannel();
            if (syncChannel == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Proxy can not found sync channel of topic[{}] ledger[{}] , will ignore check", topic, ledger);
                }
                continue;
            }
            MessageRouter router = client.fetchRouter(topic);
            if (router == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Proxy can not found message router of topic[{}] ledger[{}] , will ignore check", topic, ledger);
                }
                continue;
            }
            MessageLedger messageLedger = router.ledger(ledger);
            if (messageLedger == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Proxy can not found message ledger of topic[{}] ledger[{}] , will ignore check", topic, ledger);
                }
                continue;
            }
            List<SocketAddress> replicas = messageLedger.participants();
            if (replicas == null || replicas.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Current ledger of topic[{}] ledger[{}] is not available for proxy, will ignore check", topic, ledger);
                }
                continue;
            }
            if (replicas.contains(syncChannel.address()) && syncChannel.isActive()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Proxy can not found partition replicas of topic[{}] ledger[{}]  , will ignore check", topic, ledger);
                }
                continue;
            }
            EventExecutor executor = fixedExecutor(topic);
            try {
                executor.execute(() -> {
                    try {
                        resumeSync(syncChannel, topic, ledger, false);
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Proxy can resume-sync partition replicas of topic[{}] ledger[{}]", topic,
                                    ledger, e);
                        }
                    }
                });
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Handles synchronization messages received from client channels. Updates distribution summaries
     * and saves synchronization data to the log handler, ensuring thread-safe operations using semaphores.
     *
     * @param channel the communication channel with the client
     * @param signal the synchronization message signal containing ledger and count information
     * @param data the binary data associated with the synchronization message
     */
    @Override
    public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
        Semaphore semaphore = threadSemaphore.get();
        semaphore.acquireUninterruptibly();
        try {
            int ledger = signal.getLedger();
            int count = signal.getCount();
            DistributionSummary summary = countSummaries.get(ledger);
            if (summary == null) {
                summary = countSummaries.computeIfAbsent(ledger,
                        s -> DistributionSummary.builder(PROXY_SYNC_CHUNK_COUNT_SUMMARY_NAME)
                                .tags(Tags.of("ledger", String.valueOf(ledger))
                                        .and(BROKER_TAG, proxyConfiguration.getCommonConfiguration().getServerId())
                                        .and(CLUSTER_TAG, proxyConfiguration.getCommonConfiguration().getClusterName()))
                                .register(Metrics.globalRegistry));
            }
            summary.record(count);
            Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener(f -> semaphore.release());
            manager.getLogHandler().saveSyncData(channel.channel(), ledger, count, data, promise);
        } catch (Throwable t) {
            semaphore.release();
            logger.error(t.getMessage(), t);
        }
    }

    /**
     * Handles the event when a client channel is closed. If the client is still running,
     * it resumes the synchronization of the given channel.
     *
     * @param channel the client channel that has been closed
     */
    @Override
    public void onChannelClosed(ClientChannel channel) {
        if (!client.isRunning()) {
            return;
        }
        resumeChannelSync(channel);
    }

    /**
     * Handles the event when a topic change signal is received. The method checks
     * whether the client is running, verifies the relevant topic, schedules an
     * execution (with a random delay) to refresh the router and metadata if necessary,
     * and resumes synchronization.
     *
     * @param channel the client channel where the signal was received
     * @param signal  the topic change signal specifying details of the change
     */
    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        if (!client.isRunning()) {
            return;
        }
        String topic = signal.getTopic();
        if (!client.containsRouter(topic)) {
            return;
        }
        EventExecutor executor = fixedExecutor(topic);
        if (executor.isShuttingDown()) {
            return;
        }
        int ledger = signal.getLedger();
        int ledgerVersion = signal.getLedgerVersion();
        int randomDelay = ThreadLocalRandom.current().nextInt(proxyConfiguration.getProxyTopicChangeDelayMilliseconds());
        try {
            executor.schedule(() -> {
                try {
                    MessageRouter router = client.fetchRouter(topic);
                    if (router == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Proxy can not fetch message router of topic[{}], will ignore signal[{}]", topic, signal);
                        }
                        return;
                    }
                    MessageLedger messageLedger = router.ledger(ledger);
                    boolean refreshFailed = false;
                    if (messageLedger == null || ledgerVersion == 0 || messageLedger.version() < ledgerVersion) {
                        try {
                            client.refreshRouter(topic, channel);
                        } catch (Exception e) {
                            refreshFailed = true;
                            logger.error(e.getMessage(), e);
                        }
                        ProxyTopicHandleSupport support = (ProxyTopicHandleSupport) manager.getTopicHandleSupport();
                        support.refreshTopicMetadata(Collections.singletonList(topic), channel);
                    }
                    resumeSync(channel, topic, ledger, refreshFailed);
                    if (signal.getType() == TopicChangedSignal.Type.DELETE) {
                        noticeTopicChanged(signal);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }, randomDelay, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Notifies all active channels that the topic has changed.
     *
     * @param signal the signal containing information about the topic change
     */
    private void noticeTopicChanged(TopicChangedSignal signal) {
        Set<Channel> channels = manager.getConnection().getReadyChannels();
        if (channels == null || channels.isEmpty()) {
            return;
        }
        ByteBuf payload = null;
        try {
            for (Channel channel : channels) {
                if (!channel.isActive()) {
                    continue;
                }
                if (payload == null) {
                    payload = buildPayload(channel.alloc(), signal);
                }
                channel.writeAndFlush(payload.retainedDuplicate());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            ByteBufUtil.release(payload);
        }
    }

    /**
     * Constructs a payload for a TopicChangedSignal and writes it to a ByteBuf.
     *
     * @param alloc the ByteBufAllocator to allocate the buffer
     * @param signal the TopicChangedSignal containing the data to be written
     * @return a ByteBuf containing the constructed payload
     * @throws MeterProxyException if an error occurs while building the payload
     */
    private ByteBuf buildPayload(ByteBufAllocator alloc, TopicChangedSignal signal) {
        ByteBuf buf = null;
        try {
            int length = MessagePacket.HEADER_LENGTH + ProtoBufUtil.protoLength(signal);
            buf = alloc.ioBuffer(length);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(length);
            buf.writeInt(Command.Client.TOPIC_CHANGED);
            buf.writeInt(0);

            ProtoBufUtil.writeProto(buf, signal);
            return buf;
        } catch (Exception e) {
            ByteBufUtil.release(buf);
            throw new MeterProxyException(String.format("Proxy build signal payload error, command[%s] signal[%s]", Command.Client.TOPIC_CHANGED,  signal), e);
        }
    }

    /**
     * Handles the event when a node goes offline in the network. This method is
     * called by the framework to notify the listener about the offline state of
     * a node.
     *
     * @param channel the client channel through which the notification is received
     * @param signal  the signal containing details about the node-offline event
     */
    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        CombineListener.super.onNodeOffline(channel, signal);
    }

    /**
     * Resumes synchronization for the specified client channel.
     *
     * @param channel The client channel for which synchronization needs to be resumed.
     */
    private void resumeChannelSync(ClientChannel channel) {
        Collection<Log> logs = manager.getLogHandler().getLedgerIdOfLogs().values();
        Map<String, List<Log>> groupedLogs = logs.stream().filter(log -> channel == log.getSyncChannel()).collect(Collectors.groupingBy(Log::getTopic));
        if (groupedLogs.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<Log>> entry : groupedLogs.entrySet()) {
            String topic = entry.getKey();
            try {
                if (channel.isActive()) {
                    client.refreshRouter(topic, channel);
                } else {
                    client.refreshRouter(topic, null);
                }
                EventExecutor executor = fixedExecutor(topic);
                if (executor.isShuttingDown()) {
                    continue;
                }
                for (Log log : entry.getValue()) {
                    executor.execute(() -> resumeSync(channel, topic, log.getLedger(), false));
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Proxy resume-sync topic[{}] failure", topic, e);
                }
            }
        }
    }

    /**
     * Resumes synchronization for a given topic on a specified ledger.
     * Optionally refreshes the client router based on the active state of the provided channel.
     *
     * @param channel        the ClientChannel object representing the client's connection channel.
     * @param topic          the topic for which synchronization is to be resumed.
     * @param ledger         the integer identifier of the ledger used in the synchronization process.
     * @param refreshRouter  a boolean flag indicating whether to refresh the client router for the
     *                       specified topic based on the channel's active state.
     */
    private void resumeSync(ClientChannel channel, String topic, int ledger, boolean refreshRouter) {
        Promise<Void> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        promise.addListener(f -> {
            if (!f.isSuccess() && logger.isErrorEnabled()) {
                logger.error("Proxy resume-sync topic[{}] failure", topic, f.cause());
            }
        });
        try {
            if (refreshRouter) {
                if (channel.isActive()) {
                    client.refreshRouter(topic, channel);
                } else {
                    client.refreshRouter(topic, null);
                }
            }
            syncSupport.resumeSync(channel, topic, ledger, promise);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Selects a fixed {@link EventExecutor} based on the provided topic.
     *
     * @param topic the topic for which the executor is selected
     * @return an {@link EventExecutor} instance selected from the auxiliary event executors list
     */
    private EventExecutor fixedExecutor(String topic) {
        List<EventExecutor> executors = manager.getAuxEventExecutors();
        return executors.get((Objects.hash(topic) & 0x7fffffff) % executors.size());
    }

    /**
     * Sets the client for this ProxyClientListener.
     *
     * @param client the client to be set
     */
    public void setClient(Client client) {
        this.client = client;
    }
}
