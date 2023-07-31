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
import org.ostara.common.PartitionInfo;
import org.ostara.remote.proto.PartitionMetadata;
import org.ostara.remote.proto.TopicInfo;
import org.ostara.remote.proto.TopicMetadata;
import org.ostara.remote.util.NetworkUtils;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TopicDeletedCommand implements Command {

    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    @Override
    public String name() {
        return "deleteTopic";
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
                SocketAddress socketAddress = NetworkUtils.switchSocketAddress(address);
                ClientChannel clientChannel = client.fetchChannel(socketAddress);
                if (commandLine.hasOption('t')) {
                    String topic = commandLine.getOptionValue('t');
                    finalTopic = topic;
                    Map<String, TopicInfo> topicInfos = client.queryTopicInfos(clientChannel, topic);
                    if (topicInfos.isEmpty()) {
                        return;
                    }
                    List<PartitionInfo> infos = new ArrayList<>();
                    Iterator<TopicInfo> iterator = topicInfos.values().iterator();
                    while (iterator.hasNext()) {
                        TopicInfo topicInfo = iterator.next();
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
                    Gson gson = new Gson();
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
        }
    }
}
