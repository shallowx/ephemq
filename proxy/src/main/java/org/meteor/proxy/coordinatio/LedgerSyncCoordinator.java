package org.meteor.proxy.coordinatio;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.internal.*;
import org.meteor.configuration.ProxyConfiguration;
import org.meteor.internal.InnerClient;
import org.meteor.common.Offset;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.coordinatio.Coordinator;
import org.meteor.proxy.ProxyClientListener;
import org.meteor.proxy.ProxyLog;
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

    protected final ProxyConfiguration config;
    protected final Coordinator coordinator;
    protected final Client proxyClient;
    protected final EventExecutor taskExecutor;

    public LedgerSyncCoordinator(ProxyConfiguration config, Coordinator coordinator) {
        this.config = config;
        this.coordinator = coordinator;
        ClientConfig clientConfig = new ClientConfig();
        final List<String> upstreamServers = Arrays.stream(config.getProxyUpstreamServers()
                .split(",")).map(String::trim).toList();
        clientConfig.setBootstrapAddresses(upstreamServers);
        clientConfig.setChannelConnectionTimeoutMs(config.getProxyChannelConnectionTimeoutMs());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(1048576);
        clientConfig.setWorkerThreadCount(config.getProxyClientWorkerThreadLimit());
        clientConfig.setConnectionPoolCapacity(config.getProxyClientPoolSize());
        ProxyClientListener listener = new ProxyClientListener(config, coordinator, this);
        this.proxyClient = new InnerClient("proxy-client", clientConfig, listener, config.getCommonConfiguration(), coordinator);
        listener.setClient(proxyClient);
        this.taskExecutor = coordinator.getAuxEventExecutorGroup().next();
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
        Promise<Boolean> promise = taskExecutor.newPromise();
        promise.addListener(f -> {
            if (f.isSuccess() && (Boolean)f.getNow()) {
                coordinator.getLogManager().destroyLog(log.getLedger());
            }
        });
        log.deSyncAndCloseIfNotSubscribe(promise);
        return promise;
    }

    public Promise<Void> resumeSync(ClientChannel channel, String topic, int ledger, Promise<Void> promise) {
        Promise<Void> ret = promise == null ? taskExecutor.newPromise() : promise;
        if (channel == null) {
            ret.setSuccess(null);
            return ret;
        }

        Log log = coordinator.getLogManager().getLog(ledger);
        if (log == null) {
            ret.trySuccess(null);
            return ret;
        }

        MessageLedger messageLedger = getMessageLedger(topic, ledger);
        List<SocketAddress> replicas = messageLedger.replicas();
        if (replicas == null || replicas.isEmpty()) {
            IllegalStateException e = new IllegalStateException("No available replicas for ledger " + ledger);
            logger.error(e);
            ret.tryFailure(e);
            return ret;
        }

        if (replicas.contains(channel.address()) && channel.isActive()) {
            ret.trySuccess(null);
            return ret;
        }
        Promise<CancelSyncResponse> deSyncPromise = log.unSync(config.getProxyLeaderSyncUpstreamTimeoutMs());
        deSyncPromise.addListener(f -> {
            logger.info("Log {} is de-synced successfully", log.getLedger());
            if (f.isSuccess()) {
                try {
                   Promise<SyncResponse> syncPromise = syncLedgerFromUpstream(log, messageLedger);
                   syncPromise.addListener(future -> {
                       if (future.isSuccess()) {
                           logger.info("Log {} is sync successfully", log.getLedger());
                           ret.trySuccess(null);
                       } else {
                           logger.error("Sync Log {] failed when resuming sync, will retry after {}ms", log.getLedger(),
                                   config.getProxyResumeTaskScheduleDelayMs(), future.cause());
                           taskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), config.getProxyResumeTaskScheduleDelayMs(), TimeUnit.MILLISECONDS);
                           ret.tryFailure(future.cause());
                       }
                   });
               } catch (Exception e){
                    logger.error("Sync Log {] failed when resuming sync, will retry after {}ms", log.getLedger(),
                            config.getProxyResumeTaskScheduleDelayMs(), e);
                    taskExecutor.schedule(() -> resumeSync(channel, topic, ledger, promise), config.getProxyResumeTaskScheduleDelayMs(), TimeUnit.MILLISECONDS);
                    ret.tryFailure(e);
                }
            } else {
                logger.error("De-sync log {} failed", log.getLedger(), f.cause());
                ret.tryFailure(f.cause());
            }
        });
        return ret;
    }

    @Nonnull
    public MessageLedger getMessageLedger(String topic, int ledger) {
        MessageRouter router = proxyClient.fetchMessageRouter(topic);
        if (router == null) {
            throw new IllegalStateException(String.format("Message router not found for %s", topic));
        }
        MessageLedger messageLedger = router.ledger(ledger);
        if (messageLedger == null) {
            throw new IllegalStateException(String.format("Message ledger not found for topic=%s ledger=%d", topic, ledger));
        }
        return messageLedger;
    }

    private Promise<SyncResponse> syncLedgerFromUpstream(Log log, MessageLedger messageLedger) {
        ClientChannel channel;
        if (messageLedger == null) {
            messageLedger = getMessageLedger(log.getTopic(), log.getLedger());
        }
        channel = getSyncChannel(messageLedger);
        return log.syncFromTarget(channel, new Offset(0, 0), config.getProxyLeaderSyncUpstreamTimeoutMs());
    }

    @Nonnull
    public ClientChannel getSyncChannel(MessageLedger messageLedger) {
        List<SocketAddress> replicas = messageLedger.replicas();
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalStateException(String.format(
                    "No available replica for topic=%s ledger=%d", messageLedger.topic(), messageLedger.id()
            ));
        }
        int index = ThreadLocalRandom.current().nextInt(replicas.size());
        return getProxyClient().fetchChannel(replicas.get(index));
    }
}
