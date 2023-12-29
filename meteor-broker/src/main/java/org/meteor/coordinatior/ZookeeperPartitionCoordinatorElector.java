package org.meteor.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.meteor.config.CommonConfig;
import org.meteor.config.ZookeeperConfig;
import org.meteor.internal.PathConstants;
import org.meteor.internal.ZookeeperClient;
import org.meteor.ledger.Log;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.message.Node;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicAssignment;
import org.meteor.common.message.TopicPartition;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.listener.TopicListener;
import org.meteor.remote.proto.server.SyncResponse;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ZookeeperPartitionCoordinatorElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperPartitionCoordinatorElector.class);

    private final CommonConfig configuration;
    private final TopicPartition topicPartition;
    private final Coordinator coordinator;
    private final ParticipantCoordinator participantCoordinator;
    private final int ledger;
    private final CuratorFramework client;
    private LeaderLatch latch;

    public ZookeeperPartitionCoordinatorElector(CommonConfig brokerConfiguration, ZookeeperConfig zookeeperConfiguration, TopicPartition topicPartition, Coordinator coordinator, ParticipantCoordinator participantCoordinator, int ledger) {
        this.configuration = brokerConfiguration;
        this.topicPartition = topicPartition;
        this.coordinator = coordinator;
        this.participantCoordinator = participantCoordinator;
        this.ledger = ledger;
        this.client = ZookeeperClient.getClient(zookeeperConfiguration, configuration.getClusterName());
    }

    public void elect() throws Exception {
        String path = String.format(
                PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition()
        );
        latch = new LeaderLatch(client, path, configuration.getServerId(), LeaderLatch.CloseMode.NOTIFY_LEADER);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                if (logger.isInfoEnabled()) {
                    logger.info("Get leadership of {}, ledger={}", topicPartition, ledger);
                }
                EventExecutor poolExecutor = getPoolExecutor(topicPartition);
                poolExecutor.execute(() -> {
                    try {
                        updateTopicAssigment(path);
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Can't write leader info to zookeeper for {}", topicPartition, e);
                        }
                    }

                    for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
                        listener.onPartitionGetLeader(topicPartition);
                    }
                });
            }

            @Override
            public void notLeader() {
                if (logger.isInfoEnabled()) {
                    logger.info("Lost leadership of {}, ledger={}", topicPartition);
                }
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
                    if (logger.isInfoEnabled()) {
                        logger.info("Change leader of {} to {}", ledger, configuration.getServerId());
                    }

                    byte[] bytes = client.getData().forPath(path);
                    TopicAssignment topicAssignment = JsonMapper.deserialize(bytes, TopicAssignment.class);
                    topicAssignment.setLeader(configuration.getServerId());
                    topicAssignment.setEpoch(topicAssignment.getEpoch() + 1);
                    client.setData().forPath(path, JsonMapper.serialize(topicAssignment));

                    Log log = coordinator.getLogCoordinator().getLog(topicAssignment.getLedgerId());
                    if (log != null) {
                        log.updateEpoch(topicAssignment.getEpoch());
                    }
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Can't write leader info to zookeeper for {}, try again later", topicPartition, e);
                    }
                    poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
                }
            });
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Update topic assignment task submission failed");
            }
            poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
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

                if (logger.isInfoEnabled()) {
                    logger.info("The leader of {} is {}", ledger, leader.getId());
                }

                if (!latch.hasLeadership() && !leader.getId().equals(configuration.getServerId())) {
                    byte[] bytes = client.getData().forPath(
                            String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition())
                    );

                    TopicAssignment topicAssignment = JsonMapper.deserialize(bytes, TopicAssignment.class);
                    if (topicAssignment.getReplicas().contains(configuration.getServerId())) {
                        if (logger.isInfoEnabled()) {
                            logger.info("As the follower replica of {}, ledger={}", topicPartition, ledger);
                        }
                    }
                    syncLeader(ledger);
                }
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Try to sync ledger failed", e);
                }
                try {
                    poolExecutor.schedule(this::trySyncLeader, 50, TimeUnit.MILLISECONDS);
                } catch (Exception e1) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Retry sync ledger failed", e1);
                    }
                }
            }
        });
    }

    private void syncLeader(int ledger) throws Exception {
        Log log = coordinator.getLogCoordinator().getLog(ledger);
        Node leaderNode = coordinator.getClusterCoordinator().getClusterNode(latch.getLeader().getId());
        Client innerClient = coordinator.getInnerClient();
        ClientChannel channel = innerClient.fetchChannel(new InetSocketAddress(leaderNode.getHost(), leaderNode.getPort()));
        Promise<SyncResponse> promise = log.syncFromTarget(channel, new Offset(0, 0L), 3000);
        promise.addListener(future -> {
            if (!future.isSuccess() && logger.isErrorEnabled()) {
                logger.error("Failed to sync data as a follower", future.cause());
            }
        });
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
