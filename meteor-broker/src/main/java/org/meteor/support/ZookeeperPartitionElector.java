package org.meteor.support;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicAssignment;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.CommonConfig;
import org.meteor.config.ZookeeperConfig;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.Log;
import org.meteor.listener.TopicListener;
import org.meteor.remote.proto.server.SyncResponse;

public final class ZookeeperPartitionElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperPartitionElector.class);
    private final CommonConfig configuration;
    private final TopicPartition topicPartition;
    private final Manager coordinator;
    private final ParticipantCoordinator participantCoordinator;
    private final int ledger;
    private final CuratorFramework client;
    private LeaderLatch latch;

    public ZookeeperPartitionElector(CommonConfig brokerConfiguration, ZookeeperConfig zookeeperConfiguration,
                                     TopicPartition topicPartition, Manager coordinator,
                                     ParticipantCoordinator participantCoordinator, int ledger) {
        this.configuration = brokerConfiguration;
        this.topicPartition = topicPartition;
        this.coordinator = coordinator;
        this.participantCoordinator = participantCoordinator;
        this.ledger = ledger;
        this.client = ZookeeperClientFactory.getReadyClient(zookeeperConfiguration, configuration.getClusterName());
    }

    public void elect() throws Exception {
        String path = String.format(
                PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition()
        );
        latch = new LeaderLatch(client, path, configuration.getServerId(), LeaderLatch.CloseMode.NOTIFY_LEADER);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                EventExecutor poolExecutor = getPoolExecutor(topicPartition);
                poolExecutor.execute(() -> {
                    try {
                        updateTopicAssigment(path);
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Can't write leader info to zookeeper for topic-partition[{}]", topicPartition,
                                    e);
                        }
                    }

                    for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
                        listener.onPartitionGetLeader(topicPartition);
                    }
                });
                if (logger.isInfoEnabled()) {
                    logger.info("Get leadership of topic-partition[{}] and ledger[{}]", topicPartition, ledger);
                }
            }

            @Override
            public void notLeader() {
                EventExecutor poolExecutor = getPoolExecutor(topicPartition);
                poolExecutor.execute(() -> {
                    try {
                        Promise<Void> promise = participantCoordinator.stopChunkDispatch(ledger, null);
                        if (promise.cause() != null) {
                            if (logger.isErrorEnabled()) {
                                logger.error("Stop chunk dispatch failed", promise.cause());
                            }
                        }
                        trySyncLeader();
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Switch to follower failed", e);
                        }
                    }
                    for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
                        listener.onPartitionLostLeader(topicPartition);
                    }
                });
                if (logger.isInfoEnabled()) {
                    logger.info("Lost leadership of topic-partition[{}] and ledger[{}]", topicPartition);
                }
            }
        });

        latch.start();
        trySyncLeader();
    }

    private void updateTopicAssigment(String path) {
        EventExecutor poolExecutor = getPoolExecutor(topicPartition);
        try {
            poolExecutor.execute(() -> {
                try {
                    byte[] bytes = client.getData().forPath(path);
                    TopicAssignment topicAssignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
                    topicAssignment.setLeader(configuration.getServerId());
                    topicAssignment.setEpoch(topicAssignment.getEpoch() + 1);
                    client.setData().forPath(path, JsonFeatureMapper.serialize(topicAssignment));

                    Log log = coordinator.getLogHandler().getLog(topicAssignment.getLedgerId());
                    if (log != null) {
                        log.updateEpoch(topicAssignment.getEpoch());
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("Change leader of ledger[{}] to server-id[{}]", ledger,
                                configuration.getServerId());
                    }
                } catch (Exception e) {
                    poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
                    if (logger.isErrorEnabled()) {
                        logger.error("Can't write leader info to zookeeper for topic-partition[{}], try again later",
                                topicPartition, e);
                    }
                }
            });
        } catch (Throwable t) {
            poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
            if (logger.isErrorEnabled()) {
                logger.error("Update topic assignment task submission failed");
            }
        }
    }

    private EventExecutor getPoolExecutor(TopicPartition topicPartition) {
        List<EventExecutor> auxEventExecutors = coordinator.getAuxEventExecutors();
        return auxEventExecutors.get((Objects.hashCode(topicPartition) & 0xfffffff7) & auxEventExecutors.size());
    }

    private void trySyncLeader() {
        EventExecutor poolExecutor = getPoolExecutor(topicPartition);
        poolExecutor.execute(() -> {
            try {
                Participant leader = latch.getLeader();
                if (!leader.isLeader()) {
                    poolExecutor.schedule(this::trySyncLeader, 50, TimeUnit.MILLISECONDS);
                    return;
                }

                if (!latch.hasLeadership() && !leader.getId().equals(configuration.getServerId())) {
                    byte[] bytes = client.getData().forPath(
                            String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(),
                                    topicPartition.partition())
                    );

                    TopicAssignment topicAssignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
                    if (topicAssignment.getReplicas().contains(configuration.getServerId())) {
                        if (logger.isInfoEnabled()) {
                            logger.info("As the follower replica of topic-partition[{}] and ledger[{}]", topicPartition,
                                    ledger);
                        }
                        syncLeader(ledger);
                    }
                }

                if (logger.isInfoEnabled()) {
                    logger.info("The leader of ledger[{}] is ledger-id[{}]", ledger, leader.getId());
                }
            } catch (Exception e) {
                try {
                    poolExecutor.schedule(this::trySyncLeader, 50, TimeUnit.MILLISECONDS);
                } catch (Exception e1) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Retry sync ledger failed", e1);
                    }
                }
                if (logger.isErrorEnabled()) {
                    logger.error("Try to sync ledger failed", e);
                }
            }
        });
    }

    private void syncLeader(int ledger) throws Exception {
        Log log = coordinator.getLogHandler().getLog(ledger);
        Node leaderNode = coordinator.getClusterManager().getClusterReadyNode(latch.getLeader().getId());
        Client innerClient = coordinator.getInternalClient();
        ClientChannel channel =
                innerClient.getActiveChannel(new InetSocketAddress(leaderNode.getHost(), leaderNode.getPort()));
        Promise<SyncResponse> promise = log.syncFromTarget(channel, new Offset(0, 0L), 3000);
        CompletableFuture<SyncResponse> f = new CompletableFuture<>();
        promise.addListener(future -> {
            if (!future.isSuccess() && logger.isErrorEnabled()) {
                logger.error("Failed to sync data as a follower", future.cause());
                f.completeExceptionally(future.cause());
            } else {
                f.complete((SyncResponse) future.getNow());
            }
        });
        f.get();
    }

    public void shutdown() throws Exception {
        if (latch != null) {
            latch.close();
        }
    }

    public boolean isLeader() {
        return latch.hasLeadership();
    }
}
