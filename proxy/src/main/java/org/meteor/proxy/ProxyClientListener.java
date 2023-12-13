package org.meteor.proxy;

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
import org.meteor.client.internal.*;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.configuration.ProxyConfiguration;
import org.meteor.coordinatio.Coordinator;
import org.meteor.proxy.coordinatio.LedgerSyncCoordinator;
import org.meteor.proxy.coordinatio.ProxyTopicCoordinator;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.processor.ProcessCommand;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.ByteBufUtils;
import org.meteor.remote.util.ProtoBufUtils;
import org.meteor.ledger.Log;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.meteor.metrics.MetricsConstants.*;


public class ProxyClientListener implements ClientListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);

    private final Coordinator coordinator;
    private final LedgerSyncCoordinator syncManager;
    private Client client;
    private final ProxyConfiguration proxyConfiguration;
    private final FastThreadLocal<Semaphore> threadSemaphore = new FastThreadLocal<>() {
        @Override
        protected Semaphore initialValue() throws Exception {
            return new Semaphore(proxyConfiguration.getProxyLeaderSyncSemaphore());
        }
    };

    protected final Map<Integer, DistributionSummary> chunkCountSummaries = new ConcurrentHashMap<>();

    public ProxyClientListener(ProxyConfiguration proxyConfiguration, Coordinator manager, LedgerSyncCoordinator syncManager) {
        this.proxyConfiguration = proxyConfiguration;
        this.coordinator = manager;
        this.syncManager = syncManager;
        EventExecutor taskExecutor = manager.getAuxEventExecutorGroup().next();
        taskExecutor.scheduleWithFixedDelay(this::checkSync, proxyConfiguration.getProxySyncCheckIntervalMs(), proxyConfiguration.getProxySyncCheckIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void checkSync() {
        Map<Integer, Log> map = coordinator.getLogManager().getLedgerId2LogMap();
        if (map == null) {
            return;
        }
        Collection<Log> logs = map.values();
        for (Log log : logs) {
            String topic = log.getTopic();
            int ledger = log.getLedger();
            ClientChannel syncChannel = log.getSyncChannel();
            if (syncChannel == null) {
                continue;
            }
            MessageRouter router = client.fetchMessageRouter(topic);
            if (router == null) {
                continue;
            }
            MessageLedger messageLedger = router.ledger(ledger);
            if (messageLedger == null) {
                logger.warn("Can not find message ledger of topic={} ledger={} , will ignore check", topic, ledger);
                continue;
            }
            List<SocketAddress> replicas = messageLedger.replicas();
            if (replicas == null || replicas.isEmpty()) {
                logger.warn("Current lodger of topic={} ledger={} is not available for proxy, will ignore check", topic, ledger);
                continue;
            }
            if (replicas.contains(syncChannel.address()) && syncChannel.isActive()) {
                continue;
            }
            EventExecutor executor = fixedExecutor(topic);
            try {
                executor.execute(() -> {
                   try {
                       resumeSync(syncChannel, topic, ledger, false);
                   } catch (Exception ignored){}
                });
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
        Semaphore semaphore = threadSemaphore.get();
        semaphore.acquireUninterruptibly();
        try {
            int ledger = signal.getLedger();
            int count = signal.getCount();
            DistributionSummary summary = chunkCountSummaries.get(ledger);
            if (summary == null) {
                summary = chunkCountSummaries.computeIfAbsent(ledger,
                        s -> DistributionSummary.builder(PROXY_SYNC_CHUNK_COUNT_SUMMARY_NAME)
                        .tags(Tags.of("ledger", String.valueOf(ledger))
                                .and(BROKER_TAG, proxyConfiguration.getCommonConfiguration().getServerId())
                                .and(CLUSTER_TAG, proxyConfiguration.getCommonConfiguration().getClusterName()))
                        .register(Metrics.globalRegistry));
            }
            summary.record(count);
            Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener(f -> semaphore.release());
            coordinator.getLogManager().saveSyncData(channel.channel(), ledger, count, data, promise);
        } catch (Throwable t) {
            semaphore.release();
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    public void onChannelClosed(ClientChannel channel) {
        if (!client.isRunning()) {
            return;
        }
        resumeChannelSync(channel, true);
    }

    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        if (!client.isRunning()) {
            return;
        }
        String topic = signal.getTopic();
        if (!client.containsMessageRouter(topic)) {
            return;
        }
        EventExecutor executor = fixedExecutor(topic);
        if (executor.isShuttingDown()) {
            return;
        }
        int ledger = signal.getLedger();
        int ledgerVersion = signal.getLedgerVersion();
        int randomDelay = ThreadLocalRandom.current().nextInt(proxyConfiguration.getProxyTopicChangeDelayMs());
        try {
            executor.schedule(()-> {
               try {
                   MessageRouter router = client.fetchMessageRouter(topic);
                   if (router == null) {
                       logger.warn("Can not fetch message router of {}, will ignore signal{}", topic, signal);
                       return;
                   }
                   MessageLedger messageLedger = router.ledger(ledger);
                   boolean refreshFailed = false;
                   if (messageLedger == null || ledgerVersion == 0 || messageLedger.version() < ledgerVersion) {
                       try {
                           client.refreshMessageRouter(topic, channel);
                       } catch (Exception e) {
                           refreshFailed = true;
                           logger.error(e.getMessage(),e);
                       }
                       ProxyTopicCoordinator topicManager = (ProxyTopicCoordinator)coordinator.getTopicManager();
                       topicManager.refreshTopicMetadata(Collections.singletonList(topic), channel);
                   }
                    resumeSync(channel, topic, ledger, refreshFailed);
                   if (signal.getType() == TopicChangedSignal.Type.DELETE) {
                       noticeTopicChanged(signal);
                   }
               } catch (Exception e) {
                   logger.error(e.getMessage(), e);
               }
            }, randomDelay, TimeUnit.MILLISECONDS);
        } catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }

    private void noticeTopicChanged(TopicChangedSignal signal) {
        Set<Channel> channels = coordinator.getConnectionManager().getChannels();
        if (channels.isEmpty()) {
            return;
        }
        ByteBuf payload = null;
        try {
            for (Channel channel : channels) {
                if (!channel.isActive()) {
                    continue;
                }
                if (payload == null) {
                    payload = buildPayload(channel.alloc(), signal, ProcessCommand.Client.TOPIC_INFO_CHANGED);
                }
                channel.writeAndFlush(payload.retainedDuplicate());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            ByteBufUtils.release(payload);
        }
    }

    private ByteBuf buildPayload(ByteBufAllocator alloc, TopicChangedSignal signal, int command) {
        ByteBuf buf = null;
        try {
            int length = MessagePacket.HEADER_LENGTH + ProtoBufUtils.protoLength(signal);
            buf = alloc.ioBuffer(length);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(length);
            buf.writeInt(command);
            buf.writeInt(0);

            ProtoBufUtils.writeProto(buf, signal);
            return buf;
        } catch (Exception e){
            ByteBufUtils.release(buf);
            throw new RuntimeException(String.format("Build signal payload error, command=%d signal=%s", command, signal));
        }
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        ClientListener.super.onNodeOffline(channel, signal);
    }

    private void resumeChannelSync(ClientChannel channel, boolean refreshRouter) {
        Collection<Log> logs = coordinator.getLogManager().getLedgerId2LogMap().values();
        Map<String, List<Log>> groupedLogs = logs.stream().filter(log -> channel == log.getSyncChannel()).collect(Collectors.groupingBy(Log::getTopic));
        if (groupedLogs.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<Log>> entry : groupedLogs.entrySet()) {
            String topic = entry.getKey();
            try {
                if (refreshRouter) {
                    if (channel.isActive()) {
                        client.refreshMessageRouter(topic, channel);
                    } else {
                        client.refreshMessageRouter(topic, null);
                    }
                    EventExecutor executor = fixedExecutor(topic);
                    if (executor.isShuttingDown()) {
                        continue;
                    }
                    for (Log log : entry.getValue()) {
                        executor.execute(() -> resumeSync(channel, topic, log.getLedger(), false));
                    }
                }
            } catch (Exception e){
                logger.error("resume sync topic={} failed", topic, e);
            }
        }
    }

    private Promise<Void> resumeSync(ClientChannel channel, String topic, int ledger, boolean refreshRouter) {
        Promise<Void> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        promise.addListener(f -> {
            if (!f.isSuccess()) {
                logger.error("resume sync topic={} failed", topic, f.cause());
            }
        });
        try {
            if (refreshRouter) {
                if (channel.isActive()) {
                    client.refreshMessageRouter(topic, channel);
                } else {
                    client.refreshMessageRouter(topic, null);
                }
            }
            syncManager.resumeSync(channel, topic, ledger, promise);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            promise.tryFailure(e);
        }
        return promise;
    }

    private EventExecutor fixedExecutor(String topic) {
        List<EventExecutor> executors = coordinator.getAuxEventExecutors();
        return executors.get((Objects.hash(topic) & 0x7fffffff) % executors.size());
    }

    public void setClient(Client client) {
        this.client = client;
    }
}
