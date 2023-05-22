package org.ostara.log;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.ostara.client.internal.ClientChannel;
import org.ostara.common.Offset;
import org.ostara.common.TopicConfig;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.listener.LogListener;
import org.ostara.log.ledger.LedgerConfig;
import org.ostara.log.ledger.LedgerStorage;
import org.ostara.log.ledger.LedgerTrigger;
import org.ostara.management.Manager;
import org.ostara.metrics.MetricsConstants;
import org.ostara.remote.proto.server.CancelSyncResponse;
import org.ostara.remote.proto.server.SyncResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Log {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Log.class);
    private Config config;
    private TopicPartition topicPartition;
    private int ledger;
    private String topic;
    private LedgerStorage storage;
    private EventExecutor storageExecutor;
    private EventExecutor commandExecutor;
    private RecordEntryDispatcher entryDispatcher;
    private RecordChunkDispatcher chunkDispatcher;
    private List<LogListener> listeners;
    private Manager manager;
    private Meter segmentCountMeter;
    private Meter segmentBytesMeter;
    private int forwardTimeout = 100;
    private AtomicReference<LogState> state = new AtomicReference<>(null);
    private Migration migration;
    private ClientChannel clientChannel;
    private Promise<SyncResponse> syncFuture;
    private Promise<CancelSyncResponse> unSyncFuture;

    public Log(Config config, TopicPartition topicPartition, int ledger, int epoch, Manager manager, TopicConfig topicConfig) {
        this.config = config;
        this.topicPartition = topicPartition;
        this.ledger = ledger;
        this.topic = topicPartition.getTopic();
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
        LedgerConfig ledgerConfig;
        if (topicConfig != null) {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(topicConfig.getSegmentRetainCount())
                    .segmentBufferCapacity(topicConfig.getSegmentRollingSize())
                    .segmentRetainMs(topicConfig.getSegmentRetainMs());
        } else {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(config.getSegmentRetainCounts())
                    .segmentBufferCapacity(config.getSegmentRollingSize())
                    .segmentRetainMs(config.getSegmentRetainTime());
        }

        storageExecutor = manager.getMessageStorageEventExecutorGroup().next();
        this.storage = new LedgerStorage(ledger, topicPartition.getTopic(), epoch, ledgerConfig, storageExecutor, new InnerTrigger());
        this.manager = manager;
        this.listeners = manager.getLogManager().getLogListeners();
        Tags tags = Tags.of(MetricsConstants.TOPIC_TAG, topicPartition.getTopic())
                .and(MetricsConstants.PARTITION_TAG, String.valueOf(topicPartition.getPartition()))
                .and(MetricsConstants.BROKER_TAG, config.getServerId())
                .and(MetricsConstants.CLUSTER_TAG, config.getClusterName())
                .and(MetricsConstants.LEDGER_TAG, Integer.toString(ledger));

        this.segmentCountMeter = Gauge.builder(MetricsConstants.LOG_SEGMENT_COUNT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentCount)
                .tags(tags).register(Metrics.globalRegistry);

        this.segmentBytesMeter = Gauge.builder(MetricsConstants.LOG_SEGMENT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentBytes)
                .baseUnit("bytes")
                .tags(tags).register(Metrics.globalRegistry);

        // TODO entryDispatcher
        // TODO chunkDispatcher
    }

    public class InnerTrigger implements LedgerTrigger{
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {

        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {

        }
    }
}
