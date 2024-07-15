package org.meteor.cli.topic;

import com.google.protobuf.ProtocolStringList;
import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.cli.support.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.message.PartitionInfo;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.TopicMetadata;
import org.meteor.remote.util.NetworkUtil;

/**
 * for example:
 *
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | topic            | partition | ledger | epoch | leader | replicas | topicConfig | version | topicId |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 9         | 320    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 7         | 318    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 5         | 316    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 4         | 315    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 2         | 313    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 0         | 311    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 8         | 319    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 6         | 317    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 3         | 314    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 * | test_topic_00002 | 1         | 312    | 0     | meteor | [meteor] | [meteor]    |         | 32      |
 * +------------------+-----------+--------+-------+--------+----------+-------------+---------+---------+
 */
public class TopicDeletedCommand implements Command {
    @Override
    public String name() {
        return "topic-delete";
    }

    @Override
    public String description() {
        return "Delete topic form the broker cluster";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt =
                new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option option = new Option("t", "-topic", true, "The name of the topic to be deleted");
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
                ClientChannel clientChannel = client.getActiveChannel(socketAddress);
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
                    formatPrint(infos);
                    client.deleteTopic(topic);
                }
            }
        } catch (Exception e) {
            System.out.printf("%s [%s] ERROR %S - delete topic[%s] from cluster failure \n",
                    newDate(), Thread.currentThread().getName(), TopicDeletedCommand.class.getName(), finalTopic);
            throw new CommandException("Execution delete topic command[dt] failed", e);
        }
    }

    private void formatPrint(List<PartitionInfo> infos) {
        String[] title =
                {"topic", "partition", "ledger", "epoch", "leader", "replicas", "topicConfig", "version", "topicId"};
        FormatPrint.formatPrint(infos, title);
    }
}
