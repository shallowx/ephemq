package org.meteor.cli.topic;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.CommandException;
import org.meteor.cli.support.FormatPrint;
import org.meteor.client.core.Client;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.util.StringUtil;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.PartitionsReplicas;

/**
 * for example:
 *
 * +------------------+-----------+----------+----------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 * | topic            | partition | replicas | partitions
 *                          | config
 * |
 * +------------------+-----------+----------+----------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 * | test_topic_00000 | 10        | 1        | TopicConfig{segment_rolling_size=0, segment_retain_count=0,
 * segment_retainMs=30000, allocate=true} | {0=[meteor], 1=[meteor], 2=[meteor], 3=[meteor], 4=[meteor], 5=[meteor],
 * 6=[meteor], 7=[meteor], 8=[meteor], 9=[meteor]} |
 * +------------------+-----------+----------+----------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 *
 * +------------------+-----------+----------+------------+--------------------------------------------------------------------------------------------------------------------------+
 * | topic            | partition | replicas | partitions | config
 * |
 * +------------------+-----------+----------+------------+--------------------------------------------------------------------------------------------------------------------------+
 * | test_topic_00001 | 10        | 1        |            | {0=[meteor], 1=[meteor], 2=[meteor], 3=[meteor], 4=[meteor],
 * 5=[meteor], 6=[meteor], 7=[meteor], 8=[meteor], 9=[meteor]} |
 * +------------------+-----------+----------+------------+--------------------------------------------------------------------------------------------------------------------------+
 *
 * +------------------+-----------+----------+-------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 * | topic            | partition | replicas | partitions
 *                       | config
 * |
 * +------------------+-----------+----------+-------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 * | test_topic_00002 | 10        | 1        | TopicConfig{segment_rolling_size=0, segment_retain_count=0,
 * segment_retainMs=0, allocate=false} | {0=[meteor], 1=[meteor], 2=[meteor], 3=[meteor], 4=[meteor], 5=[meteor],
 * 6=[meteor], 7=[meteor], 8=[meteor], 9=[meteor]} |
 * +------------------+-----------+----------+-------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------+
 */
public class TopicCreatedCommand implements Command {
    @Override
    public String name() {
        return "topic-create";
    }

    @Override
    public String description() {
        return "Create topic to the broker cluster";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt =
                new Option("b", "-broker", true, "The broker address that is can connect to the broker cluster");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option explainOpt =
                new Option("e", "-explain-file", true, "The file is explain file(JSON) that will over other commands");
        explainOpt.setRequired(true);
        options.addOption(explainOpt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                if (commandLine.hasOption("e")) {
                    String explainFile = commandLine.getOptionValue('e');
                    if (!StringUtil.isNullOrEmpty(explainFile)) {
                        String content = FileUtils.readFileToString(new File(explainFile), StandardCharsets.UTF_8);
                        Gson gson = new Gson();
                        List<TopicMetadata> lists = gson.fromJson(content, new TypeToken<List<TopicMetadata>>() {
                        }.getType());
                        if (lists == null || lists.isEmpty()) {
                            return;
                        }
                        for (TopicMetadata metadata : lists) {
                            String topic = metadata.topic;
                            int partition = metadata.partition;
                            int replicas = metadata.replicas;
                            TopicConfig config = metadata.config;
                            CreateTopicResponse response = client.createTopic(topic, partition, replicas, config);
                            print(response, topic, partition, replicas, config);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Meteor-cli illegal argument exception, [-e] args cannot be empty.");
                    }
                }
            } else {
                throw new IllegalArgumentException(
                        "Meteor-cli illegal argument exception, broker-addr cannot be empty.");
            }
        } catch (Exception e) {
            System.err.println(STR."\{currentTime()} [\{Thread.currentThread()
                    .getName()}] ERROR \{TopicCreatedCommand.class.getName()} - \{e.getMessage()}");
            throw new CommandException("Execution create topic command[ct] failed", e);
        }
    }

    private void print(CreateTopicResponse response, String topic, int partition, int replica, TopicConfig config) {
        TopicMetadata metadata = new TopicMetadata();
        metadata.topic = topic;
        metadata.config = config;
        metadata.partition = partition;
        metadata.replicas = replica;

        List<PartitionsReplicas> partitionsReplicasList = response.getPartitionsReplicasList();
        metadata.partitions = partitionsReplicasList.stream()
                .collect(Collectors.toMap(
                        PartitionsReplicas::getPartition, t -> new ArrayList<>(t.getReplicasList()
                        )));

        String[] title = {"topic", "partition", "replicas", "partitions", "config"};
        List<TopicMetadata> metadatas = new ArrayList<>();
        metadatas.add(metadata);
        FormatPrint.formatPrint(metadatas, title);
    }

    private static class TopicMetadata {
        private String topic;
        private int partition;
        private int replicas;
        private TopicConfig config;
        private Map<Integer, List<String>> partitions;

        public TopicMetadata() {
        }

        public TopicMetadata(String topic, int partition, int replicas, TopicConfig config,
                             Map<Integer, List<String>> partitions) {
            this.topic = topic;
            this.partition = partition;
            this.replicas = replicas;
            this.config = config;
            this.partitions = partitions;
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

        public int getReplicas() {
            return replicas;
        }

        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }

        public TopicConfig getConfig() {
            return config;
        }

        public void setConfig(TopicConfig config) {
            this.config = config;
        }

        public Map<Integer, List<String>> getPartitions() {
            return partitions;
        }

        public void setPartitions(Map<Integer, List<String>> partitions) {
            this.partitions = partitions;
        }

        @Override
        public String toString() {
            return STR."(topic='\{topic}', partition=\{partition}, replicas=\{replicas}, config=\{config}, partitions=\{partitions})";
        }
    }
}
