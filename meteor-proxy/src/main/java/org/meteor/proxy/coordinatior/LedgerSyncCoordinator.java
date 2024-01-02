package org.meteor.proxy.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.internal.*;
import org.meteor.proxy.internal.ProxyConfig;
import org.meteor.internal.InternalClient;
import org.meteor.common.message.Offset;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.coordinatior.Coordinator;
import org.meteor.proxy.internal.ProxyClientListener;
import org.meteor.proxy.internal.ProxyLog;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.ledger.Log;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public abstract class LedgerSyncCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerSyncCoordinator.class);

    protected final ProxyConfig config;
    protected final Coordinator coordinator;
    protected final Client proxyClient;
    protected final EventExecutor resumeSyncTaskExecutor;

    public LedgerSyncCoordinator(ProxyConfig config, Coordinator coordinator) {
        this.config = config;
        this.coordinator = coordinator;
        ClientConfig clientConfig = new ClientConfig();
        final List<String> upstreamServers = Arrays.stream(config.getProxyUpstreamServers()
                .split(",")).map(String::trim).toList();
        clientConfig.setBootstrapAddresses(upstreamServers);
        clientConfig.setChannelConnectionTimeoutMilliseconds(config.getProxyChannelConnectionTimeoutMilliseconds());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(1048576);
        clientConfig.setWorkerThreadLimit(config.getProxyClientWorkerThreadLimit());
        clientConfig.setConnectionPoolCapacity(config.getProxyClientPoolSize());
        ProxyClientListener listener = new ProxyClientListener(config, coordinator, this);
        this.proxyClient = new InternalClient("proxy-client", clientConfig, listener, config.getCommonConfiguration(), coordinator);
        listener.setClient(proxyClient);
        this.resumeSyncTaskExecutor = coordinator.getAuxEventExecutorGroup().next();
    }

    public void start() {
        this.proxyClient.start();
    }

    public void shutDown() {
        this.proxyClient.close();
    }

    public Client getProxyClient() {
        return this.proxyClient;
    }

    public Promise<Boolean> deSyncAndCloseIfNotSubscribe(ProxyLog log) {
        Promise<Boolean> promise = resumeSyncTaskExecutor.newPromise();
        promise.addListener(f -> {
            if (f.isSuccess() && (Boolean)f.getNow()) {
                coordinator.getLogCoordinator().destroyLog(log.getLedger());
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

        Log log = coordinator.getLogCoordinator().getLog(ledger);
        if (log == null) {
            ret.trySuccess(null);
            return ret;
        }

        MessageLedger messageLedger = getMessageLedger(topic, ledger);
        List<SocketAddress> replicas = messageLedger.participants();
        if (replicas == null || replicas.isEmpty()) {
            IllegalStateException e = new IllegalStateException("No available replicas for ledger[" + ledger + "]");
            logger.error(e);
            ret.tryFailure(e);
            return ret;
        }

        if (replicas.contains(channel.address()) && channel.isActive()) {
            ret.trySuccess(null);
            return ret;
        }
        Promise<CancelSyncResponse> cancelSyncPromise = log.cancelSync(config.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
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
                                       config.getProxyResumeTaskScheduleDelayMilliseconds(), future.cause());
                           }
                           resumeSyncTaskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), config.getProxyResumeTaskScheduleDelayMilliseconds(), TimeUnit.MILLISECONDS);
                           ret.tryFailure(future.cause());
                       }
                   });
               } catch (Exception e){
                    if (logger.isErrorEnabled()) {
                        logger.error("Sync Log[{}] failed when resuming sync, will retry after {} ms", log.getLedger(),
                                config.getProxyResumeTaskScheduleDelayMilliseconds(), e);
                    }
                    resumeSyncTaskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), config.getProxyResumeTaskScheduleDelayMilliseconds(), TimeUnit.MILLISECONDS);
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
            throw new IllegalStateException(String.format("The topic[%s] message ledger not found which ledger[%d]", topic, ledger));
        }
        return messageLedger;
    }

    private Promise<SyncResponse> syncLedgerFromUpstream(Log log, MessageLedger messageLedger) {
        ClientChannel channel;
        if (messageLedger == null) {
            messageLedger = getMessageLedger(log.getTopic(), log.getLedger());
        }
        channel = getSyncChannel(messageLedger);
        return log.syncFromTarget(channel, new Offset(0, 0), config.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
    }

    @Nonnull
    public ClientChannel getSyncChannel(MessageLedger messageLedger) {
        List<SocketAddress> replicas = messageLedger.participants();
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "No available replica which topic=%s ledger=%d", messageLedger.topic(), messageLedger.id()
            ));
        }
        int index = ThreadLocalRandom.current().nextInt(replicas.size());
        return getProxyClient().fetchChannel(replicas.get(index));
    }
}
