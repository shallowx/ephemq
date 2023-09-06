package org.ostara.cli.topic;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;
import org.ostara.common.TopicConfig;
import org.ostara.common.util.StringUtils;
import org.ostara.remote.proto.server.CreateTopicResponse;
import org.ostara.remote.proto.server.PartitionsReplicas;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("all")
public class TopicCreatedCommand implements Command {
    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    @Override
    public String name() {
        return "createTopic";
    }

    @Override
    public String description() {
        return "create topic";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "broker address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option explainOpt = new Option("e", "explain file", true, "topic to which file(JSON)");
        explainOpt.setRequired(true);
        options.addOption(explainOpt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        try {
            if (commandLine.hasOption('b')) {
                if (commandLine.hasOption('e')) {
                    String explainFile = commandLine.getOptionValue('e');
                    if (!StringUtils.isNullOrEmpty(explainFile)) {
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
                            String print = print(response, topic, partition, replicas, config);
                            System.out.printf("%s [%s] INFO %s - %s \n", newDate(), Thread.currentThread().getName(), TopicCreatedCommand.class.getName(), print);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.printf("%s [%s] ERROR %s - %s \n", newDate(), Thread.currentThread().getName(), TopicDeletedCommand.class.getName(), e.getCause().getMessage());
        }
    }

    private String print(CreateTopicResponse response, String topic, int partition, int replica, TopicConfig config) {
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
        Gson gSon = new Gson();
        return gSon.toJson(metadata);
    }

    private static class TopicMetadata {
        private String topic;
        private int partition;
        private int replicas;
        private TopicConfig config;
        private Map<Integer, List<String>> partitions;

        public TopicMetadata() {
        }

        public TopicMetadata(String topic, int partition, int replicas, TopicConfig config, Map<Integer, List<String>> partitions) {
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
            return "{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", replicas=" + replicas +
                    ", config=" + config +
                    ", partitions=" + partitions +
                    '}';
        }
    }
}
