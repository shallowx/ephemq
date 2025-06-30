package org.meteor.cli.topic;

import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.ledger.MigrateLedgerPlanCommand;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.cli.support.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * TopicListCommand is a class that implements the Command interface,
 * allowing users to query and retrieve topic information from a broker cluster.
 * for example:
 * <p>
 * +---------------+-----------+--------+-------+--------+----------+
 * | topic         | partition | ledger | epoch | leader | replicas |
 * +---------------+-----------+--------+-------+--------+----------+
 * | #test#default | 0         | 1      | 0     | meteor | [meteor] |
 * +---------------+-----------+--------+-------+--------+----------+
 */
public class TopicListCommand implements Command {
    /**
     * Returns the name of the TopicListCommand.
     *
     * @return the string "topic-info" representing the name of the command
     */
    @Override
    public String name() {
        return "topic-info";
    }

    /**
     * Provides a brief description of the `TopicListCommand` which retrieves
     * topic information from the broker cluster.
     *
     * @return a string containing a short description of the command.
     */
    @Override
    public String description() {
        return "Get topic info from the broker cluster";
    }

    /**
     * Builds and returns a set of command line options with additional options specific
     * to the TopicListCommand.
     *
     * @param options the initial set of command line options to be built upon
     * @return the complete set of command line options for the TopicListCommand
     */
    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt =
                new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option topicOpt = new Option("t", "-topic", true, "The name is used to query topic info");
        topicOpt.setRequired(false);
        options.addOption(topicOpt);

        Option ledgerOpt = new Option("l", "-ledger", true, "The ledger id that is use to filter the topic info");
        ledgerOpt.setRequired(false);
        options.addOption(ledgerOpt);

        Option partitionOpt =
                new Option("p", "-partition", true, "The partition id that is use to filter the topic info");
        partitionOpt.setRequired(false);
        options.addOption(partitionOpt);

        return options;
    }

    /**
     * Executes the command to query topic information based on the provided command line arguments, options, and client.
     *
     * @param commandLine the command line arguments containing the options to execute the query.
     * @param options     the options available for the command line interface.
     * @param client      the client used to execute the query on the server.
     * @throws Exception if an error occurs while executing the command.
     */
    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                String address = commandLine.getOptionValue("b").trim();
                if (StringUtil.isNullOrEmpty(address)) {
                    throw new IllegalStateException();
                }
                SocketAddress socketAddress = NetworkUtil.switchSocketAddress(address);
                ClientChannel clientChannel = client.getActiveChannel(socketAddress);
                List<TopicMetadata> topics;
                Map<String, TopicInfo> topicInfos;
                if (commandLine.hasOption("t")) {
                    String topic = commandLine.getOptionValue("t").trim();
                    topicInfos = client.queryTopicInfos(clientChannel, topic);

                } else {
                    topicInfos = client.queryTopicInfos(clientChannel);
                }
                if (topicInfos == null || topicInfos.isEmpty()) {
                    System.out.printf("%s %s INFO %s - Topic info is empty", currentTime(), Thread.currentThread().getName(), TopicListCommand.class.getName());
                    return;
                }

                topics = topicInfos.values().stream().map(topicInfo -> {
                    Map<Integer, PartitionMetadata> partitionsMap = topicInfo.getPartitionsMap();
                    if (!partitionsMap.isEmpty()) {
                        return partitionsMap.values().stream().map(pm -> {
                            ProtocolStringList replicaNodeIdsList = pm.getReplicaNodeIdsList();
                            List<String> replicaNodeIds = new ArrayList<>(replicaNodeIdsList);

                            return new TopicMetadata(topicInfo.getTopic().getName(), pm.getId(), pm.getLedger(),
                                    pm.getEpoch(), pm.getLeaderNodeId(), replicaNodeIds);
                        }).findAny().get();
                    }
                    return null;
                }).toList();

                if (commandLine.hasOption('l')) {
                    int ledger = Integer.parseInt(commandLine.getOptionValue('l'));
                    topics = topics.stream().filter(t -> t.getLedger() == ledger).toList();
                }

                if (commandLine.hasOption('p')) {
                    int partition = Integer.parseInt(commandLine.getOptionValue('p'));
                    topics = topics.stream().filter(t -> t.getPartition() == partition).toList();
                }
                formatPrint(topics);
            } else {
                throw new IllegalArgumentException(
                        "Meteor-cli illegal argument exception, broker-addr cannot be empty.");
            }
        } catch (Throwable t) {
            System.out.printf("%s %s ERROR - %s - %s", currentTime(), Thread.currentThread().getName(), MigrateLedgerPlanCommand.class.getName(), t.getMessage());
            throw new CommandException("Execution query topic infos command[topics] failed", t);
        }
    }

    /**
     * Formats and prints the metadata of a list of topics. The formatted output includes the
     * topic, partition, ledger, epoch, leader, and replicas data, which is printed in a tabular format.
     *
     * @param topics the list of TopicMetadata objects to be formatted and printed
     */
    private void formatPrint(List<TopicMetadata> topics) {
        String[] title = {"topic", "partition", "ledger", "epoch", "leader", "replicas"};
        FormatPrint.formatPrint(topics, title);
    }

    private static class TopicMetadata {
        /**
         * The topic name associated with this metadata entry.
         */
        private String topic;
        /**
         * Represents the specific partition associated with a topic within the metadata information.
         */
        private int partition;
        /**
         * Represents the ledger identifier for a particular partition
         * within a topic in the TopicMetadata class.
         */
        private int ledger;
        /**
         * The epoch number for the topic partition, which provides a logical clock
         * for updates and can be used to determine the order of changes.
         */
        private int epoch;
        /**
         * The leader of the partition for the topic.
         * This is typically the node or server instance that is
         * responsible for handling the read and write requests for the partition.
         */
        private String leader;
        /**
         * List of replica identifiers for a given partition in the topic metadata.
         */
        private List<String> replicas;

        /**
         * Constructs a new TopicMetadata object representing metadata for a specific topic partition.
         *
         * @param topic the name of the topic.
         * @param partition the partition number of the topic.
         * @param ledger the ledger identifier of the partition.
         * @param epoch the epoch number of the partition.
         * @param leader the leader node id of the partition.
         * @param replicas the list of node ids that are replicas for the partition.
         */
        public TopicMetadata(String topic, int partition, int ledger, int epoch, String leader, List<String> replicas) {
            this.topic = topic;
            this.partition = partition;
            this.ledger = ledger;
            this.epoch = epoch;
            this.leader = leader;
            this.replicas = replicas;
        }

        /**
         * Retrieves the topic associated with this metadata.
         *
         * @return the topic as a String.
         */
        public String getTopic() {
            return topic;
        }

        /**
         * Sets the topic name for the topic metadata.
         *
         * @param topic the name of the topic
         */
        public void setTopic(String topic) {
            this.topic = topic;
        }

        /**
         * Returns the partition number for the topic metadata.
         *
         * @return the partition number
         */
        public int getPartition() {
            return partition;
        }

        /**
         * Sets the partition number for the topic.
         *
         * @param partition the partition number to be set
         */
        public void setPartition(int partition) {
            this.partition = partition;
        }

        /**
         * Retrieves the ledger identifier.
         *
         * @return the ledger identifier as an integer.
         */
        public int getLedger() {
            return ledger;
        }

        /**
         * Sets the ledger value for the TopicMetadata instance.
         *
         * @param ledger the ledger identifier to be set
         */
        public void setLedger(int ledger) {
            this.ledger = ledger;
        }

        /**
         *
         */
        public int getEpoch() {
            return epoch;
        }

        /**
         * Sets the epoch value associated with this topic metadata.
         *
         * @param epoch the new epoch value to set
         */
        public void setEpoch(int epoch) {
            this.epoch = epoch;
        }

        /**
         * Retrieves the leader of the topic.
         *
         * @return the leader of the topic.
         */
        public String getLeader() {
            return leader;
        }

        /**
         * Sets the leader for the topic partition.
         *
         * @param leader the leader to set for the topic partition
         */
        public void setLeader(String leader) {
            this.leader = leader;
        }

        /**
         * Retrieves the list of replicas for the topic metadata.
         *
         * @return a List of Strings representing the replicas.
         */
        public List<String> getReplicas() {
            return replicas;
        }

        /**
         * Sets the list of replicas for the topic metadata.
         *
         * @param replicas the list of replica nodes
         */
        public void setReplicas(List<String> replicas) {
            this.replicas = replicas;
        }

        /**
         * Returns a string representation of the TopicMetadata object.
         *
         * @return a string representation of the TopicMetadata object, including the topic,
         *         partition, ledger, epoch, leader, and replicas.
         */
        @Override
        public String toString() {
            return "TopicMetadata (topic='%s', partition=%d, ledger=%d, epoch=%d, leader='%s', replicas=%s)".formatted(topic, partition, ledger, epoch, leader, replicas);
        }
    }
}
