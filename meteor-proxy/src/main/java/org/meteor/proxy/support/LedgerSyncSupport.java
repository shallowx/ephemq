package org.meteor.proxy.support;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.internal.InternalClient;
import org.meteor.ledger.Log;
import org.meteor.proxy.core.ProxyClientListener;
import org.meteor.proxy.core.ProxyConfig;
import org.meteor.proxy.core.ProxyLog;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.support.Manager;

/**
 * Abstract base class for supporting ledger synchronization in a proxy environment.
 * <p>
 * This class provides foundational functionalities required for synchronizing
 * ledgers between different nodes in a proxy setup. It handles initialization
 * of the proxy client, resume tasks executor, and other configurations necessary
 * for ledger synchronization.
 */
public abstract class LedgerSyncSupport {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerSyncSupport.class);
    /**
     * Holds the configuration settings for the proxy.
     * This variable is used to manage how the ledger sync process interacts with the proxy.
     */
    protected final ProxyConfig proxyConfig;
    /**
     * The Manager instance used for handling various server components and interactions within the LedgerSyncSupport class.
     * Provides capabilities such as starting and shutting down operations and accessing supporting components like
     * topic handles, cluster management, logging, connections, and event executors.
     */
    protected final Manager manager;
    /**
     * The proxyClient variable holds an instance of the Client used to communicate with an external proxy.
     * This client is critical for performing synchronization tasks with upstream ledgers.
     * It is initialized with configurations provided by the proxyConfig field.
     */
    protected final Client proxyClient;
    /**
     * An EventExecutor used to manage and control the execution of tasks related to resuming ledger synchronization
     * in the context of the LedgerSyncSupport class. This ensures that resuming synchronization tasks are handled
     * asynchronously and efficiently.
     */
    protected final EventExecutor resumeSyncTaskExecutor;

    /**
     * Initializes a new instance of the LedgerSyncSupport class.
     * This sets up the proxy client and its configurations using the provided ProxyConfig and Manager instances.
     *
     * @param config The ProxyConfig instance containing configuration details for the proxy client.
     * @param manager The Manager instance that manages various tasks and configurations for the application.
     */
    public LedgerSyncSupport(ProxyConfig config, Manager manager) {
        this.proxyConfig = config;
        this.manager = manager;
        ClientConfig clientConfig = new ClientConfig();
        final List<String> upstreamServers = Arrays.stream(config.getProxyUpstreamServers()
                .split(",")).map(String::trim).toList();
        clientConfig.setBootstrapAddresses(upstreamServers);
        clientConfig.setChannelConnectionTimeoutMilliseconds(config.getProxyChannelConnectionTimeoutMilliseconds());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(1048576);
        clientConfig.setWorkerThreadLimit(config.getProxyClientWorkerThreadLimit());
        clientConfig.setConnectionPoolCapacity(config.getProxyClientPoolSize());
        ProxyClientListener listener = new ProxyClientListener(config, manager, this);
        this.proxyClient =
                new InternalClient("proxy-client", clientConfig, listener, config.getCommonConfiguration());
        listener.setClient(proxyClient);
        this.resumeSyncTaskExecutor = manager.getAuxEventExecutorGroup().next();
    }

    /**
     * Attempts to cancel the synchronisation process and close the log if
     * it is not subscribed. If this operation is successful and the log
     * is not subscribed, the log is destroyed.
     *
     * @param log the ProxyLog that will undergo the de-synchronisation and close operation
     * @return a promise that resolves to {@code true} if the operation is successful, otherwise {@code false}
     */
    public Promise<Boolean> deSyncAndCloseIfNotSubscribe(ProxyLog log) {
        Promise<Boolean> promise = resumeSyncTaskExecutor.newPromise();
        promise.addListener(f -> {
            if (f.isSuccess() && (Boolean) f.getNow()) {
                manager.getLogHandler().destroyLog(log.getLedger());
            }
        });
        log.cancelSyncAndCloseIfNotSubscribe(promise);
        return promise;
    }

    /**
     * Resumes synchronization for a given ledger if the current channel is not active or not among the ledger's participants.
     *
     * @param channel The client channel that is used for synchronization.
     * @param topic The topic associated with the ledger.
     * @param ledger The identifier of the ledger to be synchronized.
     * @param promise The external promise to be fulfilled upon synchronization task completion.
     * @return A promise that will be completed when the synchronization task is done.
     */
    public Promise<Void> resumeSync(ClientChannel channel, String topic, int ledger, Promise<Void> promise) {
        Promise<Void> ret = promise == null ? resumeSyncTaskExecutor.newPromise() : promise;
        if (channel == null) {
            ret.setSuccess(null);
            return ret;
        }

        Log log = manager.getLogHandler().getLog(ledger);
        if (log == null) {
            ret.trySuccess(null);
            return ret;
        }

        MessageLedger messageLedger = getMessageLedger(topic, ledger);
        List<SocketAddress> addresses = messageLedger.participants();
        if (addresses == null || addresses.isEmpty()) {
            IllegalStateException e =
                    new IllegalStateException(STR."No available participant's address for ledger[\{ledger}]");
            logger.error(e);
            ret.tryFailure(e);
            return ret;
        }

        if (addresses.contains(channel.address()) && channel.isActive()) {
            ret.trySuccess(null);
            return ret;
        }
        Promise<CancelSyncResponse> cancelSyncPromise = log.cancelSync(proxyConfig.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
        cancelSyncPromise.addListener(f -> {
            if (logger.isInfoEnabled()) {
                logger.info("Log[{}] is de-synced successfully", log.getLedger());
            }

            if (f.isSuccess()) {
                try {
                    Promise<SyncResponse> syncPromise = syncLedgerFromUpstream(log, messageLedger);
                    syncPromise.addListener(future -> {
                        if (future.isSuccess()) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Log[{}] is sync successfully", log.getLedger());
                            }
                            ret.trySuccess(null);
                        } else {
                            if (logger.isErrorEnabled()) {
                                logger.error("Sync Log[{}] failed when resuming sync, will retry after {} ms", log.getLedger(),
                                        proxyConfig.getProxyResumeTaskScheduleDelayMilliseconds(), future.cause());
                            }
                            resumeSyncTaskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), proxyConfig.getProxyResumeTaskScheduleDelayMilliseconds(), TimeUnit.MILLISECONDS);
                            ret.tryFailure(future.cause());
                        }
                    });
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Sync Log[{}] failed when resuming sync, will retry after {} ms", log.getLedger(),
                                proxyConfig.getProxyResumeTaskScheduleDelayMilliseconds(), e);
                    }
                    resumeSyncTaskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), proxyConfig.getProxyResumeTaskScheduleDelayMilliseconds(), TimeUnit.MILLISECONDS);
                    ret.tryFailure(e);
                }
            } else {
                if (logger.isErrorEnabled()) {
                    logger.error("De-sync log[{}] failed", log.getLedger(), f.cause());
                }
                ret.tryFailure(f.cause());
            }
        });
        return ret;
    }

    /**
     * Retrieves the MessageLedger for a specific topic and ledger.
     *
     * @param topic the topic for which to retrieve the MessageLedger
     * @param ledger the ledger identifier within the topic to retrieve
     * @return the MessageLedger associated with the specified topic and ledger
     * @throws IllegalStateException if the message router or the ledger for the specified topic and ledger is not found
     */
    @Nonnull
    public MessageLedger getMessageLedger(String topic, int ledger) {
        MessageRouter router = proxyClient.fetchRouter(topic);
        if (router == null) {
            throw new IllegalStateException(STR."The topic[\{topic}] message router not found");
        }
        MessageLedger messageLedger = router.ledger(ledger);
        if (messageLedger == null) {
            throw new IllegalStateException(STR."The topic[\{topic}] message ledger[\{ledger}] not found");
        }
        return messageLedger;
    }

    /**
     * Synchronizes the ledger from the upstream server.
     *
     * @param log The log object containing the topic and ledger information.
     * @param messageLedger The message ledger object for synchronization. If null, it will be fetched based on the log's topic and ledger.
     * @return A Promise that resolves to a SyncResponse object containing the synchronization results.
     */
    private Promise<SyncResponse> syncLedgerFromUpstream(Log log, MessageLedger messageLedger) {
        ClientChannel channel;
        if (messageLedger == null) {
            messageLedger = getMessageLedger(log.getTopic(), log.getLedger());
        }
        channel = getSyncChannel(messageLedger);
        return log.syncFromTarget(channel, new Offset(0, 0), proxyConfig.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
    }

    /**
     * Retrieves an active client channel that can be used for synchronization.
     * It selects a random participant from the provided message ledger's participants.
     * If no participants are available, an IllegalStateException is thrown.
     *
     * @param messageLedger the message ledger containing participants' information
     * @return a ClientChannel instance representing an active channel for synchronization
     * @throws IllegalStateException if no participants are available in the message ledger
     */
    @Nonnull
    public ClientChannel getSyncChannel(MessageLedger messageLedger) {
        List<SocketAddress> addresses = messageLedger.participants();
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalStateException(
                    STR."No available participants that it's the topic[\{messageLedger.topic()}] and the ledger[\{messageLedger.id()}]");
        }
        int index = ThreadLocalRandom.current().nextInt(addresses.size());
        return proxyClient.getActiveChannel(addresses.get(index));
    }

    /**
     * Retrieves the proxy client instance.
     *
     * @return the proxy client instance used by the LedgerSyncSupport class.
     */
    Client getProxyClient() {
        return this.proxyClient;
    }

    /**
     * Starts the LedgerSyncSupport by invoking the start method of the proxy client.
     * This method is used to initialize and begin the synchronization process for ledgers.
     * It ensures that the proxy client, which handles the actual synchronization logic,
     * is properly started and ready to operate.
     *
     * Note: This method does not perform any direct synchronization operations itself.
     * It delegates the responsibility to the proxyClient's start() method.
     */
    public void start() {
        this.proxyClient.start();
    }

    /**
     * Shuts down the ledger synchronization support components gracefully.
     *
     * This method closes the proxy client connection and ensures that resources are released properly.
     */
    public void shutDown() {
        this.proxyClient.close();
    }
}
