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

public abstract class LedgerSyncCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerSyncCoordinator.class);

    protected final ProxyConfig proxyConfig;
    protected final Manager manager;
    protected final Client proxyClient;
    protected final EventExecutor resumeSyncTaskExecutor;

    public LedgerSyncCoordinator(ProxyConfig config, Manager manager) {
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
                new InternalClient("proxy-client", clientConfig, listener, config.getCommonConfiguration(), manager);
        listener.setClient(proxyClient);
        this.resumeSyncTaskExecutor = manager.getAuxEventExecutorGroup().next();
    }

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
            IllegalStateException e = new IllegalStateException("No available participant's address for ledger[" + ledger + "]");
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

    @Nonnull
    public MessageLedger getMessageLedger(String topic, int ledger) {
        MessageRouter router = proxyClient.fetchRouter(topic);
        if (router == null) {
            throw new IllegalStateException(String.format("The topic[%s] message router not found", topic));
        }
        MessageLedger messageLedger = router.ledger(ledger);
        if (messageLedger == null) {
            throw new IllegalStateException(String.format("The topic[%s] message ledger[%d not found", topic, ledger));
        }
        return messageLedger;
    }

    private Promise<SyncResponse> syncLedgerFromUpstream(Log log, MessageLedger messageLedger) {
        ClientChannel channel;
        if (messageLedger == null) {
            messageLedger = getMessageLedger(log.getTopic(), log.getLedger());
        }
        channel = getSyncChannel(messageLedger);
        return log.syncFromTarget(channel, new Offset(0, 0), proxyConfig.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
    }

    @Nonnull
    public ClientChannel getSyncChannel(MessageLedger messageLedger) {
        List<SocketAddress> addresses = messageLedger.participants();
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "No available participants that it's the topic[%s] and the ledger[%d]", messageLedger.topic(), messageLedger.id()
            ));
        }
        int index = ThreadLocalRandom.current().nextInt(addresses.size());
        return proxyClient.getActiveChannel(addresses.get(index));
    }

    Client getProxyClient() {
        return this.proxyClient;
    }

    public void start() {
        this.proxyClient.start();
    }

    public void shutDown() {
        this.proxyClient.close();
    }
}
