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
 * The TopicCreatedCommand class implements the Command interface and is responsible for creating a topic
 * in the broker cluster. This command uses various options and parameters to connect to the broker,
 * read configurations from an explain file, and execute the topic creation process.
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
    /**
     * Returns the name of the command, which is "topic-create".
     *
     * @return the name of the command
     */
    @Override
    public String name() {
        return "topic-create";
    }

    /**
     * Provides a brief description of the command, which involves
     * creating a topic in the broker cluster.
     *
     * @return a string containing a short description of the command.
     */
    @Override
    public String description() {
        return "Create topic to the broker cluster";
    }

    /**
     * Builds and returns a set of command line options by adding specific options
     * required for the TopicCreatedCommand.
     *
     * @param options the initial set of command line options to be built upon
     * @return the complete set of command line options specific to the command
     */
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

    /**
     * Executes the command to create topics based on the provided command line options.
     *
     * @param commandLine The command line arguments parsed into options.
     * @param options     The options available for this command.
     * @param client      The client instance used to interact with the system to create topics.
     * @throws Exception if an error occurs during the execution of the command.
     */
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

    /**
     * Processes the given CreateTopicResponse and prints out the metadata for the created topic,
     * including topic name, partition number, replica count, partitions information, and configuration.
     *
     * @param response the response object containing information about the created topic
     * @param topic the name of the created topic
     * @param partition the number of partitions for the created topic
     * @param replica the number of replicas for the created topic
     * @param config the configuration settings for the created topic
     */
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
        /**
         * Represents the topic name associated with the metadata.
         */
        private String topic;
        /**
         * Represents the partition index for the topic.
         */
        private int partition;
        /**
         * Represents the number of replica nodes for a topic partition.
         */
        private int replicas;
        /**
         * The configuration settings for a specific topic.
         */
        private TopicConfig config;
        /**
         * A map that associates partition numbers with lists of replica assignments.
         * The keys represent partition numbers, while the values are lists of replica strings.
         */
        private Map<Integer, List<String>> partitions;

        /**
         * Default constructor for TopicMetadata.
         * Initializes a new instance of the TopicMetadata class with default values.
         */
        public TopicMetadata() {
        }

        /**
         * Constructor for TopicMetadata.
         *
         * @param topic       The name of the topic.
         * @param partition   The partition number of the topic.
         * @param replicas    The number of replicas for the topic.
         * @param config      The configuration for the topic.
         * @param partitions  A map of partition numbers to a list of their corresponding replicas.
         */
        public TopicMetadata(String topic, int partition, int replicas, TopicConfig config,
                             Map<Integer, List<String>> partitions) {
            this.topic = topic;
            this.partition = partition;
            this.replicas = replicas;
            this.config = config;
            this.partitions = partitions;
        }

        /**
         * Retrieves the topic name.
         *
         * @return the name of the topic
         */
        public String getTopic() {
            return topic;
        }

        /**
         * Sets the topic name.
         *
         * @param topic the name of the topic to set
         */
        public void setTopic(String topic) {
            this.topic = topic;
        }

        /**
         * Retrieves the partition number of the topic.
         *
         * @return the partition number
         */
        public int getPartition() {
            return partition;
        }

        /**
         * Sets the partition number for the topic.
         *
         * @param partition the partition number to set
         */
        public void setPartition(int partition) {
            this.partition = partition;
        }

        /**
         * Retrieves the number of replicas for this topic partition.
         *
         * @return the number of replicas
         */
        public int getReplicas() {
            return replicas;
        }

        /**
         * Sets the number of replicas for the topic.
         *
         * @param replicas the number of replicas to set
         */
        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }

        /**
         * Retrieves the configuration settings of the topic.
         *
         * @return the TopicConfig object containing the configuration settings of the topic.
         */
        public TopicConfig getConfig() {
            return config;
        }

        /**
         * Sets the configuration for the topic.
         *
         * @param config the topic configuration to set
         */
        public void setConfig(TopicConfig config) {
            this.config = config;
        }

        /**
         * Retrieves the partition information for the topic.
         *
         * @return a map where the keys are partition numbers and the values are lists of replica IDs.
         */
        public Map<Integer, List<String>> getPartitions() {
            return partitions;
        }

        /**
         * Sets the partitions map for the topic metadata.
         *
         * @param partitions a map where the key is the partition number and the value is a list of strings representing the replicas for that partition
         */
        public void setPartitions(Map<Integer, List<String>> partitions) {
            this.partitions = partitions;
        }

        /**
         * Returns a string representation of the TopicMetadata instance.
         *
         * @return a string containing the topic, partition, replicas, config, and partitions details
         */
        @Override
        public String toString() {
            return STR."(topic='\{topic}', partition=\{partition}, replicas=\{replicas}, config=\{config}, partitions=\{partitions})";
        }
    }
}
