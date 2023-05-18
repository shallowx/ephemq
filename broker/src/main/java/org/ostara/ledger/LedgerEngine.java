package org.ostara.ledger;

import static org.ostara.remote.RemoteException.Failure.MESSAGE_APPEND_EXCEPTION;
import static org.ostara.remote.RemoteException.Failure.SUBSCRIBE_EXCEPTION;
import static org.ostara.remote.RemoteException.of;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Subscription;
import org.ostara.internal.config.ServerConfig;
import org.ostara.internal.metadata.CachingClusterNode;
import org.ostara.internal.metrics.LedgerMetricsListener;
import org.ostara.remote.util.NetworkUtils;

@ThreadSafe
public class LedgerEngine {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerEngine.class);

    private final ServerConfig config;
    private final Int2ObjectMap<Ledger> ledgers = new Int2ObjectOpenHashMap<>();
    private final List<LedgerMetricsListener> listeners = new LinkedList<>();
    private final EventExecutor retryExecutor;
    private final CachingClusterNode nodeCacheWriterSupport;

    public LedgerEngine(ServerConfig config, CachingClusterNode clusterNodeCacheSupport) {
        this.config = config;
        this.nodeCacheWriterSupport = clusterNodeCacheSupport;
        this.retryExecutor = NetworkUtils.newEventExecutorGroup(1, "ledger-init-retry").next();
    }

    public void start() throws Exception {
        for (LedgerMetricsListener listener : listeners) {
            listener.startUp(nodeCacheWriterSupport.getThisNode());
        }
    }

    public void addListeners(LedgerMetricsListener listener) {
        this.listeners.add(listener);
    }

    public void initLog(String topic, int partitionId, int epoch, int ledgerId) {
        try {
            Ledger ledger = ledgers.computeIfAbsent(ledgerId, k -> {
                Ledger newLedger = new Ledger(config, topic, partitionId, ledgerId, epoch, listeners);

                try {

                    newLedger.start();

                } catch (Exception e) {
                    throw new LedgerException(e.getMessage());
                }

                for (LedgerMetricsListener listener : listeners) {
                    // analyze and resolve exceptions by {@link org.leopard.internal.metrics
                    // .LedgerMetricsListener#onInitLedger}
                    listener.onInitLedger(newLedger);
                }
                return newLedger;
            });

            logger.info("Initialize log successfully, topic={} partitionId={} ledgerId={}", topic,
                    ledger.getPartition(), ledger.getLedgerId());
        } catch (Throwable t) {
            String error = t instanceof LedgerException ?
                    String.format("Ledger init failure and try again, topic=%s partitionId=%d ledgerId=%d", topic,
                            partitionId, ledgerId) : t.getMessage();
            if (t instanceof LedgerException) {
                retryExecutor.execute(() -> initLog(topic, partitionId, epoch, ledgerId));
            }

            logger.error(error);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void subscribe(Channel channel, String topic, String queue, short version, int ledgerId, int epoch,
                          long index, Promise<Subscription> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(of(SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }

            checkLedgerState(ledger);

            ledger.subscribe(channel, topic, queue, version, epoch, index, promise);
        } catch (Throwable t) {
            logger.error("Failed to subscribe, channel={} topic={} queue={} version={} epoch={} index={}",
                    channel.toString(), ledger.getTopic(), queue, version, epoch, index);
            promise.tryFailure(t);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void clean(Channel channel, String topic, String queue, int ledgerId, Promise<Void> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(of(SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }
            checkLedgerState(ledger);

            ledger.clean(channel, topic, queue, promise);
        } catch (Throwable t) {
            logger.error("Failed to clean subscribe, channel={} topic={} queue={} version={} epoch={} index={}",
                    channel.toString(), ledger.getTopic(), queue);
            promise.tryFailure(t);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void append(int ledgerId, String queue, ByteBuf payload, short version, Promise<Offset> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(of(MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }

            checkLedgerState(ledger);

            for (LedgerMetricsListener listener : listeners) {
                // analyze and resolve exceptions by {@link org.leopard.internal.metrics
                // .LedgerMetricsListener#onReceiveMessage}
                listener.onReceiveMessage(ledger.getTopic(), queue, ledger.getLedgerId(), 1);
            }

            ledger.append(queue, version, payload, promise);
        } catch (Throwable t) {
            logger.error("Failed to append message, topic={} queue={} version={}, error:{}", ledger.getTopic(),
                    queue, version, t);
            promise.tryFailure(t);
        }
    }

    public void clearChannel(Channel channel) {
        if (ledgers.isEmpty()) {
            return;
        }

        ObjectCollection<Ledger> activeLedgers = ledgers.values();
        for (Ledger ledger : activeLedgers) {
            ledger.clearChannel(channel);
        }
    }

    public Ledger getLedger(int ledgerId) {
        return ledgers.get(ledgerId);
    }

    private void checkLedgerState(Ledger ledger) {
        Ledger.State state = ledger.getState();
        if (state != Ledger.State.STARTED) {
            throw new RuntimeException(String.format("Ledger %d not active", ledger.getLedgerId()));
        }
    }

    public void close() throws Exception {
        if (!ledgers.isEmpty()) {
            ledgers.values().forEach(Ledger::close);
        }

        logger.warn("Ledger manager close is starting...");
    }
}
