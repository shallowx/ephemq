package org.meteor.cli.topic;

import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.Command;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.message.PartitionInfo;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.TopicMetadata;
import org.meteor.remote.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicDeletedCommand implements Command {
    @Override
    public String name() {
        return "dt";
    }

    @Override
    public String description() {
        return "delete topic form broker";
    }

    @Override
    public Options buildOptions(Options options) {
        Option option = new Option("t", "topic", true, "topic name");
        option.setRequired(true);
        options.addOption(option);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        String finalTopic = null;
        try {
            if (commandLine.hasOption('b')) {
                String address = commandLine.getOptionValue("b").trim();
                if (StringUtil.isNullOrEmpty(address)) {
                    throw new IllegalStateException();
                }
                SocketAddress socketAddress = NetworkUtil.switchSocketAddress(address);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);
                if (commandLine.hasOption('t')) {
                    String topic = commandLine.getOptionValue('t');
                    finalTopic = topic;
                    Map<String, TopicInfo> topicInfos = client.queryTopicInfos(clientChannel, topic);
                    if (topicInfos.isEmpty()) {
                        return;
                    }
                    List<PartitionInfo> infos = new ArrayList<>();
                    for (TopicInfo topicInfo : topicInfos.values()) {
                        TopicMetadata metadata = topicInfo.getTopic();
                        Map<Integer, PartitionMetadata> partitionsMap = topicInfo.getPartitionsMap();
                        for (Map.Entry<Integer, PartitionMetadata> entry : partitionsMap.entrySet()) {
                            PartitionMetadata pm = entry.getValue();
                            ProtocolStringList replicaNodeIds = pm.getReplicaNodeIdsList();
                            infos.add(new PartitionInfo(
                                    topic,
                                    metadata.getId(),
                                    pm.getId(),
                                    pm.getLedger(),
                                    pm.getEpoch(),
                                    pm.getLeaderNodeId(),
                                    new HashSet<>(new ArrayList<>(replicaNodeIds)),
                                    null,
                                    pm.getVersion()
                            ));
                        }
                    }
                    System.out.printf("%s [%s] INFO %S - print topic[%s] metadata options: %s \n",
                            newDate(), Thread.currentThread().getName(), TopicDeletedCommand.class.getName(), topic, gson.toJson(infos));
                    TimeUnit.MILLISECONDS.sleep(1000);
                    client.deleteTopic(topic);
                    System.out.printf("%s [%s] INFO %S - delete topic[%s] from cluster successfully \n",
                            newDate(), Thread.currentThread().getName(), TopicDeletedCommand.class.getName(), topic);
                }
            }
        } catch (Exception e) {
            System.out.printf("%s [%s] ERROR %S - delete topic[%s] from cluster failure \n",
                    newDate(), Thread.currentThread().getName(), TopicDeletedCommand.class.getName(), finalTopic);
            throw new RuntimeException(e);
        }
    }
}
