package org.meteor.cli.topic;

import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.core.Command;
import org.meteor.cli.core.CommandException;
import org.meteor.cli.core.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.util.NetworkUtil;

/**
 * for example:
 *
 * +---------------+-----------+--------+-------+--------+----------+
 * | topic         | partition | ledger | epoch | leader | replicas |
 * +---------------+-----------+--------+-------+--------+----------+
 * | #test#default | 0         | 1      | 0     | meteor | [meteor] |
 * +---------------+-----------+--------+-------+--------+----------+
 */
public class TopicListCommand implements Command {
    @Override
    public String name() {
        return "topics";
    }

    @Override
    public String description() {
        return "Get topic info from the broker cluster";
    }

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
                    System.out.printf("%s [%s] INFO %s - Topic info is empty \n", newDate(),
                            Thread.currentThread().getName(), TopicListCommand.class.getName());
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
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(),
                    TopicListCommand.class.getName(), t.getMessage());
            throw new CommandException("Execution query topic infos command[topics] failed", t);
        }
    }

    private void formatPrint(List<TopicMetadata> topics) {
        String[] title = {"topic", "partition", "ledger", "epoch", "leader", "replicas"};
        FormatPrint.formatPrint(topics, title);
    }

    private static class TopicMetadata {
        private String topic;
        private int partition;
        private int ledger;
        private int epoch;
        private String leader;
        private List<String> replicas;

        public TopicMetadata(String topic, int partition, int ledger, int epoch, String leader, List<String> replicas) {
            this.topic = topic;
            this.partition = partition;
            this.ledger = ledger;
            this.epoch = epoch;
            this.leader = leader;
            this.replicas = replicas;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public int getLedger() {
            return ledger;
        }

        public void setLedger(int ledger) {
            this.ledger = ledger;
        }

        public int getEpoch() {
            return epoch;
        }

        public void setEpoch(int epoch) {
            this.epoch = epoch;
        }

        public String getLeader() {
            return leader;
        }

        public void setLeader(String leader) {
            this.leader = leader;
        }

        public List<String> getReplicas() {
            return replicas;
        }

        public void setReplicas(List<String> replicas) {
            this.replicas = replicas;
        }

        @Override
        public String toString() {
            return "TopicMetadata{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", ledger=" + ledger +
                    ", epoch=" + epoch +
                    ", leader='" + leader + '\'' +
                    ", replicas=" + replicas +
                    '}';
        }
    }
}
