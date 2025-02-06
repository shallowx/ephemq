package org.meteor.support;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The ZookeeperPartitionElector class handles the leader election for topic partitions using Zookeeper.
 * It uses CuratorFramework to interact with Zookeeper and LeaderLatch to handle the leadership election process.
 * The class ensures that leadership transitions are properly managed and synchronizes leader information.
 */
public final class ZookeeperPartitionElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperPartitionElector.class);
    /**
     * Holds the configuration settings required for the ZookeeperPartitionElector.
     * Immutable once initialized to ensure consistency. It contains essential server
     * and partitioning configurations wrapped in the CommonConfig object.
     */
    private final CommonConfig configuration;
    /**
     * Represents the specific topic and partition for which the leadership election is being managed.
     * This field is utilized throughout the {@code ZookeeperPartitionElector} class to identify the
     * target partition in various methods, such as starting the election process, updating topic assignments,
     * and synchronizing the leader state.
     */
    private final TopicPartition topicPartition;
    /**
     * The manager instance responsible for handling various server components and their interactions
     * within the context of Zookeeper-based partition elections.
     * This manager is initialized and controlled within the ZookeeperPartitionElector class.
     */
    private final Manager manager;
    /**
     * The ParticipantSupport instance manages tasks related to participants
     * and synchronization within a distributed system. It is responsible for
     * operations like starting the participant support, subscribing to ledgers,
     * and managing synchronization with the leader.
     */
    private final ParticipantSupport participantSupport;
    /**
     * The ledger ID associated with the ZookeeperPartitionElector.
     * This variable uniquely identifies the ledger being used for
     * the current partition election process.
     */
    private final int ledger;
    /**
     * The CuratorFramework client used for interacting with Zookeeper.
     * This client handles the management and communication with the Zookeeper server,
     * including creating, updating, and deleting Zookeeper nodes.
     * It is a crucial component for the leader election and synchronization mechanisms
     * within the ZookeeperPartitionElector class.
     */
    private final CuratorFramework client;
    /**
     * A LeaderLatch instance that is used to manage and control leadership election for the partition.
     * It ensures that only one participant at a time can be the leader in a distributed system.
     */
    private LeaderLatch latch;

    /**
     * Constructs an instance of {@code ZookeeperPartitionElector} which is responsible for managing
     * the election of a leader for a partition in a distributed system. This involves interacting
     * with Zookeeper to ensure that one participant is elected as the leader for a specified partition.
     *
     * @param brokerConfiguration    The common configuration settings for the broker.
     * @param zookeeperConfiguration Configuration settings specific to Zookeeper.
     * @param topicPartition         The topic and partition for which the election is being conducted.
     * @param manager                Manages various operations required for the election process.
     * @param participantSupport     Provides support operations for participants in the election.
     * @param ledger                 The ledger ID used for tracking the election state.
     */
    public ZookeeperPartitionElector(CommonConfig brokerConfiguration, ZookeeperConfig zookeeperConfiguration,
                                     TopicPartition topicPartition, Manager manager,
                                     ParticipantSupport participantSupport, int ledger) {
        this.configuration = brokerConfiguration;
        this.topicPartition = topicPartition;
        this.manager = manager;
        this.participantSupport = participantSupport;
        this.ledger = ledger;
        this.client = ZookeeperClientFactory.getReadyClient(zookeeperConfiguration, configuration.getClusterName());
    }

    /**
     * Initiates the leadership election process for a specific topic partition.
     * This method uses a ZooKeeper-based LeaderLatch to manage the leadership for the topic partition.
     * When called, it sets up a leader latch listener to handle the events of becoming a leader
     * or losing the leadership.
     * <p>
     * The process includes:
     * - Formatting the path for the topic partition in ZooKeeper.
     * - Initializing a LeaderLatch with the server ID and NOTIFY_LEADER close mode.
     * - Adding a listener to handle leadership events (isLeader, notLeader).
     * - Starting the latch and attempting to synchronize the leader.
     *
     * @throws Exception if an error occurs during the election process or while interacting with ZooKeeper.
     */
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
                    for (TopicListener listener : manager.getTopicHandleSupport().getTopicListener()) {
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
                        Promise<Void> promise = participantSupport.stopChunkDispatch(ledger, null);
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
                    for (TopicListener listener : manager.getTopicHandleSupport().getTopicListener()) {
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

    /**
     * Updates the topic assignment for a specified path.
     *
     * This method retrieves the topic assignment data, updates the leader to
     * the current server, increments the epoch, and writes the updated data
     * back. If errors occur, it handles them by rescheduling the task.
     *
     * @param path The path from which to retrieve and update the topic assignment.
     */
    private void updateTopicAssigment(String path) {
        EventExecutor poolExecutor = getPoolExecutor(topicPartition);
        try {
            poolExecutor.execute(() -> {
                try {
                    byte[] bytes = client.getData().forPath(path);
                    TopicAssignment topicAssignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
                    topicAssignment.setLeader(configuration.getServerId());
                    topicAssignment.setEpoch(topicAssignment.getEpoch() + 1);
                    client.setData().forPath(path, SerializeFeatureSupport.serialize(topicAssignment));

                    Log log = manager.getLogHandler().getLog(topicAssignment.getLedgerId());
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

    /**
     * Retrieves an EventExecutor from a pool based on the hash code of a TopicPartition.
     *
     * @param topicPartition The TopicPartition instance used to determine which EventExecutor to retrieve.
     * @return The selected EventExecutor based on the hash code of the provided TopicPartition.
     */
    private EventExecutor getPoolExecutor(TopicPartition topicPartition) {
        List<EventExecutor> auxEventExecutors = manager.getAuxEventExecutors();
        return auxEventExecutors.get((Objects.hashCode(topicPartition) & 0xfffffff7) & auxEventExecutors.size());
    }

    /**
     * Attempts to synchronize the leader for the given topic partition by verifying the current leader
     * and scheduling retries if necessary. The method utilizes an executor to perform the
     *  task asynchronously.
     *
     * The leader election and synchronization process involves the following steps:
     * 1. Retrieve the leader participant from the latch.
     * 2. Check if the current node is the leader.
     * 3. If not, schedule a retry after 50 milliseconds.
     * 4. If the current node is not the leader but a follower, retrieve the topic assignment data.
     * 5. Check if the current node is part of the replica set.
     * 6. If it's a part of the replica set, log the info and synchronize with the leader's state.
     * 7. Log the leader's ID after successful synchronization.
     * 8. In case of an exception, schedule a retry and log errors if retries fail.
     *
     * This method is designed for internal use within the ZookeeperPartitionElector class.
     *
     * @throws Exception if the synchronization process or data retrieval fails, an exception is caught and logged.
     */
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

                    TopicAssignment topicAssignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
                    if (topicAssignment.getReplicas().contains(configuration.getServerId())) {
                        if (logger.isInfoEnabled()) {
                            logger.info("As the follower replica of topic-partition[{}] and ledger[{}]", topicPartition,
                                    ledger);
                        }
                        syncLeader(ledger);
                    }
                }

                if (logger.isInfoEnabled()) {
                    logger.info(STR."The leader of the ledger[\{ledger}] is \{leader.getId()}");
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

    /**
     * Syncs the leader node by fetching the log data from the target leader node
     * and synchronizing it with the current follower node.
     *
     * @param ledger the identifier of the ledger to synchronize
     * @throws Exception if any error occurs during the synchronization process
     */
    private void syncLeader(int ledger) throws Exception {
        Log log = manager.getLogHandler().getLog(ledger);
        Node leaderNode = manager.getClusterManager().getClusterReadyNode(latch.getLeader().getId());
        Client innerClient = manager.getInternalClient();
        ClientChannel channel =
                innerClient.getActiveChannel(new InetSocketAddress(leaderNode.getHost(), leaderNode.getPort()));
        Promise<SyncResponse> promise = log.syncFromTarget(channel, new Offset(0, 0L), 3000);
        CompletableFuture<SyncResponse> f = new CompletableFuture<>();
        promise.addListener(future -> {
            if (!future.isSuccess()) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to sync data as a follower", future.cause());
                }
                f.completeExceptionally(future.cause());
            } else {
                f.complete((SyncResponse) future.getNow());
            }
        });
        f.get();
    }

    /**
     * Shuts down the ZookeeperPartitionElector instance.
     * <p>
     * This method closes the latch associated with this elector if it is not null.
     * It is typically used to release the resources and clean up the internal state.
     *
     * @throws Exception if an error occurs during shutdown.
     */
    public void shutdown() throws Exception {
        if (latch != null) {
            latch.close();
        }
    }

    /**
     * Checks if the current instance holds the leadership.
     *
     * @return true if the current instance has leadership, false otherwise
     */
    public boolean isLeader() {
        return latch.hasLeadership();
    }
}
