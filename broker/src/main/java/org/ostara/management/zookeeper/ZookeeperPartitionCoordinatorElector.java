package org.ostara.management.zookeeper;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.common.Node;
import org.ostara.common.Offset;
import org.ostara.common.TopicAssignment;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.listener.TopicListener;
import org.ostara.management.JsonMapper;
import org.ostara.management.Manager;
import org.ostara.management.ParticipantManager;
import org.ostara.remote.proto.server.SyncResponse;
import org.ostara.storage.Log;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZookeeperPartitionCoordinatorElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperPartitionCoordinatorElector.class);

    private final CoreConfig config;
    private final TopicPartition topicPartition;
    private final Manager manager;
    private final ParticipantManager replicaManager;
    private final int ledger;
    private final CuratorFramework client;
    private LeaderLatch latch;

    public ZookeeperPartitionCoordinatorElector(CoreConfig config, TopicPartition topicPartition, Manager manager, ParticipantManager replicaManager, int ledger) {
        this.config = config;
        this.topicPartition = topicPartition;
        this.manager = manager;
        this.replicaManager = replicaManager;
        this.ledger = ledger;
        this.client = ZookeeperClient.getClient(config, config.getClusterName());
    }

    public void elect() throws Exception {
        String path = String.format(
                PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition()
        );
        latch = new LeaderLatch(client, path, config.getServerId(), LeaderLatch.CloseMode.NOTIFY_LEADER);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                logger.info("Get leadership of {}, ledger={}", topicPartition, ledger);

                EventExecutor poolExecutor = getPoolExecutor(topicPartition);
                poolExecutor.execute(() -> {
                    try {
                        updateTopicAssigment(path);
                    } catch (Exception e) {
                        logger.error("Can't write leader info to zookeeper for {}", topicPartition, e);
                    }

                    for (TopicListener listener : manager.getTopicManager().getTopicListener()) {
                        listener.onPartitionGetLeader(topicPartition);
                    }
                });
            }

            @Override
            public void notLeader() {
                logger.info("Lost leadership of {}, ledger={}", topicPartition);
                EventExecutor poolExecutor = getPoolExecutor(topicPartition);
                poolExecutor.execute(() -> {
                    try {
                        Promise<Void> promise = replicaManager.stopChunkDispatch(ledger, null);
                        if (promise.cause() != null) {
                            logger.error("Stop chunk dispatch failed", promise.cause());
                        }
                        trySyncLeader();
                    } catch (Exception e) {
                        logger.error("Switch to follower failed", e);
                    }
                    for (TopicListener listener : manager.getTopicManager().getTopicListener()) {
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
                    logger.info("Change leader of {} to {}", ledger, config.getServerId());

                    byte[] bytes = client.getData().forPath(path);
                    TopicAssignment topicAssignment = JsonMapper.deserialize(bytes, TopicAssignment.class);
                    topicAssignment.setLeader(config.getServerId());
                    topicAssignment.setEpoch(topicAssignment.getEpoch() + 1);
                    client.setData().forPath(path, JsonMapper.serialize(topicAssignment));

                    Log log = manager.getLogManager().getLog(topicAssignment.getLedgerId());
                    if (log != null) {
                        log.updateEpoch(topicAssignment.getEpoch());
                    }
                } catch (Exception e) {
                    logger.error("Can't write leader info to zookeeper for {}, try again later", topicPartition, e);
                    poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
                }
            });
        } catch (Throwable t) {
            logger.error("Update topic assignment task submission failed");
            poolExecutor.schedule(() -> updateTopicAssigment(path), 50, TimeUnit.MILLISECONDS);
        }
    }

    private EventExecutor getPoolExecutor(TopicPartition topicPartition) {
        List<EventExecutor> auxEventExecutors = manager.getAuxEventExecutors();
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
                logger.info("The leader of {} is {}", ledger, leader.getId());

                if (!latch.hasLeadership() && !leader.getId().equals(config.getServerId())) {
                    byte[] bytes = client.getData().forPath(
                            String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition())
                    );

                    TopicAssignment topicAssignment = JsonMapper.deserialize(bytes, TopicAssignment.class);
                    if (topicAssignment.getReplicas().contains(config.getServerId())) {
                        logger.info("As the follower replica of {}, ledger={}", topicPartition, ledger);
                    }
                    syncLeader(ledger);
                }
            } catch (Exception e) {
                logger.error("Try to sync ledger failed", e);

                try {
                    poolExecutor.schedule(this::trySyncLeader, 50, TimeUnit.MILLISECONDS);
                } catch (Exception e1) {
                    logger.error("Retry sync ledger failed", e1);
                }
            }
        });
    }

    private void syncLeader(int ledger) throws Exception {
        Log log = manager.getLogManager().getLog(ledger);
        Node leaderNode = manager.getClusterManager().getClusterNode(latch.getLeader().getId());
        Client innerClient = manager.getInnerClient();
        ClientChannel channel = innerClient.fetchChannel(new InetSocketAddress(leaderNode.getHost(), leaderNode.getPort()));
        Promise<SyncResponse> promise = log.syncFromTarget(channel, new Offset(0, 0L), 3000);
        promise.addListener(future -> {
            if (!future.isSuccess()) {
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

    public String getCurrentLeader() throws Exception {
        Participant leader = latch.getLeader();
        if (leader.isLeader()) {
            return leader.getId();
        }

        return null;
    }

    public Set<String> getParticipants() throws Exception {
        Collection<Participant> participants = latch.getParticipants();
        return participants.stream().map(Participant::getId).collect(Collectors.toSet());
    }
}
