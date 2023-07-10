package org.ostara.cli.topic;

import com.google.gson.Gson;
import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.remote.proto.PartitionMetadata;
import org.ostara.remote.proto.TopicInfo;
import org.ostara.remote.util.NetworkUtils;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TopicListCommand implements Command {
    @Override
    public String name() {
        return "TopicList";
    }

    @Override
    public String description() {
        return "get topics from server";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "broker address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option clusterOpt = new Option("c", "cluster", true, "which cluster name");
        clusterOpt.setRequired(true);
        options.addOption(clusterOpt);

        Option topicOpt = new Option("t", "topic", true, "which topic name");
        topicOpt.setRequired(true);
        options.addOption(topicOpt);

        Option ledgerOpt = new Option("l", "ledger", true, "which ledger id");
        ledgerOpt.setRequired(true);
        options.addOption(ledgerOpt);

        Option partitionOpt = new Option("p", "partition", true, "which partition id");
        partitionOpt.setRequired(true);
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
                SocketAddress socketAddress = NetworkUtils.switchSocketAddress(address);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);
                List<TopicMetadata> topics = new ArrayList<>();
                if (commandLine.hasOption("t")) {
                    String topic = commandLine.getOptionValue("t").trim();
                    Map<String, TopicInfo> topicInfos = StringUtil.isNullOrEmpty(topic)
                            ? client.queryTopicInfos(clientChannel, topic)
                            : client.queryTopicInfos(clientChannel);

                    if (topicInfos == null || topicInfos.isEmpty()) {
                        System.out.printf("%s [%s] INFO %s - Topic info is empty \n", newDate(), Thread.currentThread().getName(), TopicListCommand.class.getName());
                        return;
                    }
                    topics = topicInfos.values().stream().map(topicInfo -> {
                        Map<Integer, PartitionMetadata> partitionsMap = topicInfo.getPartitionsMap();
                        if (!partitionsMap.isEmpty()) {
                            return partitionsMap.values().stream().map(pm -> {
                                ProtocolStringList replicaNodeIdsList = pm.getReplicaNodeIdsList();
                                List<String> replicaNodeIds = new ArrayList<>(replicaNodeIdsList);

                                return new TopicMetadata(topicInfo.getTopic().getName(), pm.getId(), pm.getLedger(), pm.getEpoch(), pm.getLeaderNodeId(), replicaNodeIds);
                            }).findAny().get();
                        }
                        return null;
                    }).toList();
                }

                if (commandLine.hasOption('l')) {
                    int ledger = Integer.parseInt(commandLine.getOptionValue('l'));
                    topics = topics.stream().filter(t -> t.getLedger() == ledger).toList();
                }

                if (commandLine.hasOption('p')) {
                    int partition = Integer.parseInt(commandLine.getOptionValue('p'));
                    topics = topics.stream().filter(t -> t.getPartition() == partition).toList();
                }

                Gson gson = new Gson();
                System.out.printf("%s [%s] INFO %s - %s \n", newDate(), Thread.currentThread().getName(), TopicListCommand.class.getName(), gson.toJson(topics));
            }
        } catch (Throwable t){
            System.out.printf("%s [%s] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), TopicListCommand.class.getName(), t.getCause().getMessage());
        }
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
            return "{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", ledger=" + ledger +
                    ", epoch=" + epoch +
                    ", leader='" + leader + '\'' +
                    ", replicas=" + replicas +
                    '}';
        }
    }

    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return   format.format(new Date());
    }
}
